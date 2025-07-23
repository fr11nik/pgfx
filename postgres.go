// Package pgfx предоставляет удобный доступ к PostgreSQL с поддержкой пулов соединений и управления транзакциями через менеджер транзакций и контекст.
package pgfx

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	_defaultMaxPoolSize  = 6
	_defaultConnAttempts = 10
	_defaultConnTimeout  = time.Millisecond * 250
)

type QueryExecutor interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
	Transactor
}

// Postgres представляет собой обёртку над pgxpool.Pool, предоставляющую удобный доступ
// к PostgreSQL с поддержкой пулов соединений и управления транзакциями.
//
// Структура содержит два основных компонента:
//   - Pool: стандартный пул соединений от pgxpool, используемый для обычных операций (запросы, exec и т.д.).
//   - TransactionalPool: интерфейс QueryExecutor, который поддерживает выполнение транзакций.
//     Внутри он может указывать на обычный пул или на активную транзакцию, что позволяет
//     использовать один и тот же код как внутри, так и вне транзакции.
//
// Для управления транзакциями (особенно вложенные или передаваемые между слоями) рекомендуется
// использовать метод NewTransactionManager(), который возвращает TxManager — объект,
// упрощающий работу с транзакциями через контекст и функциональные опции.
type Postgres struct {
	// Pool — это стандартный пул соединений pgxpool.Pool.
	// Используется для выполнения обычных SQL-операций вне транзакций.
	// Также служит базовым соединением для запуска новых транзакций.
	//
	// Пример использования:
	//   rows, _ := pg.Pool.Query(ctx, "SELECT id FROM users")
	Pool *pgxpool.Pool
	// TransactionalPool — это интерфейс QueryExecutor, совместимый как с *pgxpool.Pool,
	// так и с *pgx.Tx. Это позволяет использовать один и тот же код
	// для выполнения запросов как внутри, так и вне транзакции.
	//
	// По умолчанию указывает на Pool, но при запуске транзакции через TxManager
	// временно заменяется на объект транзакции (*pgx.Tx), обеспечивая согласованность.
	//
	// Этот интерфейс используется в методах, которым важно быть "транзакционно-безопасными",
	// например, в сервисах, где логика может вызываться как в рамках транзакции, так и без неё.
	TransactionalPool QueryExecutor
	maxPoolSize       int32
	connAttempts      int32
	connTimeout       time.Duration
	qt                pgx.QueryTracer
}

// New create postgres instance
func New(connStr string, opts ...Option) (*Postgres, error) {
	pg := &Postgres{
		maxPoolSize:  _defaultMaxPoolSize,
		connAttempts: _defaultConnAttempts,
		connTimeout:  _defaultConnTimeout,
	}

	for _, opt := range opts {
		opt(pg)
	}

	poolConfig, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("postgres - NewPostgres - pgxpool.ParseConfig: %w", err)
	}

	poolConfig.MaxConns = pg.maxPoolSize
	poolConfig.ConnConfig.ConnectTimeout = pg.connTimeout
	poolConfig.ConnConfig.Tracer = pg.qt
	for pg.connAttempts > 0 {
		pg.Pool, err = pgxpool.NewWithConfig(context.Background(), poolConfig)

		if err == nil {
			break
		}

		log.Printf("Postgres is trying to connect, attempts left: %d", pg.connAttempts)

		time.Sleep(pg.connTimeout)

		pg.connAttempts--
	}

	if err != nil {
		return nil, fmt.Errorf("postgres - NewPostgres - connAttempts == 0: %w", err)
	}

	if pg.qt != nil {
		if err := otelpgx.RecordStats(pg.Pool); err != nil {
			return nil, fmt.Errorf("unable to record database stats: %w", err)
		}
	}
	transactor := pgTransactor{dbc: pg.Pool}
	pg.TransactionalPool = transactor

	return pg, nil
}

// NewTransactionManager создаёт новый менеджер транзакций (TxManager),
// который использует TransactionalPool из текущего экземпляра Postgres.
//
// TxManager позволяет легко запускать транзакции, в том числе вложенные (с savepoint),
// и передавать их через контекст между разными уровнями приложения (например, service → repository).
//
// Когда транзакция активна, все операции, использующие TransactionalPool,
// автоматически выполняются внутри этой транзакции.
//
// Пример:
//
//	txManager := pg.NewTransactionManager()
//	err := txManager.ReadCommited(ctx, func(ctx context.Context) error {
//	    err := someRepoWithTransactionalPool.DoSomething(ctx)
//	    return err
//	})
//
// Важно: для выполнения запросов внутри транзакций следует использовать pg.TransactionalPool,
// а не pg.Pool напрямую.
func (p *Postgres) NewTransactionManager() TxManager {
	return newTransactionManager(p.TransactionalPool)
}

// GetDBForTransactionManager возвращает обертку базы данных через которую можно вызывать запросы.
// Эта обертка нужна для работы с TransactionManager. TransacionManager работает только когда методы внутри ReadCommited используют QueryExecutor
//
// Пример:
//
//	type someRepoWithTransactionalPool struct {
//		db pgfx.QueryExecutor
//	}
//
//	func(s someRepoWithTransactionalPool) DoSomething(ctx context.Context) error {
//		_, err := s.db.Exec(ctx, "INSERT INTO users (id, name) VALUES (1, 'John')")
//	}
//
//	txManager := pgfx.NewTransactionManager()
//	err := txManager.ReadCommited(ctx, func(ctx context.Context) error {
//	    err := someRepoWithTransactionalPool.DoSomething(ctx)
//	    return err
//	})
func (p *Postgres) GetDBForTransactionManager() QueryExecutor {
	return p.TransactionalPool
}

// Close is close postgres pool
func (p *Postgres) Close() error {
	if p.Pool != nil {
		p.Pool.Close()
	}
	return nil
}
