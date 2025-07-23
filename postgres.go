// Package pgfx -
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

// Postgres -.
type Postgres struct {
	Pool *pgxpool.Pool
	// used for context transactions
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
	// poolConfig.ConnConfig.Tracer = otelpgx.NewTracer()
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

// Close is close postgres pool
func (p *Postgres) Close() error {
	if p.Pool != nil {
		p.Pool.Close()
	}
	return nil
}
