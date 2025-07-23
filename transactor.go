package pgfx

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// pgTransactor -.
type pgTransactor struct {
	dbc *pgxpool.Pool
}

func (p pgTransactor) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	tx, ok := ctx.Value(TxKey).(pgx.Tx)
	if ok {
		return tx.Exec(ctx, sql, args...)
	}

	return p.dbc.Exec(ctx, sql, args...)
}

func (p pgTransactor) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	tx, ok := ctx.Value(TxKey).(pgx.Tx)
	if ok {
		return tx.Query(ctx, sql, args...)
	}

	return p.dbc.Query(ctx, sql, args...)
}

func (p pgTransactor) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	tx, ok := ctx.Value(TxKey).(pgx.Tx)
	if ok {
		return tx.QueryRow(ctx, sql, args...)
	}

	return p.dbc.QueryRow(ctx, sql, args...)
}

func (p pgTransactor) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	tx, ok := ctx.Value(TxKey).(pgx.Tx)
	if ok {
		return tx.CopyFrom(ctx, tableName, columnNames, rowSrc)
	}
	return p.dbc.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

func (p pgTransactor) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	return p.dbc.BeginTx(ctx, txOptions)
}

func (p pgTransactor) Ping(ctx context.Context) error {
	return p.dbc.Ping(ctx)
}

func (p pgTransactor) Close() {
	p.dbc.Close()
}
