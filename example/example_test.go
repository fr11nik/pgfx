package pgfx_test

import (
	"context"
	"fmt"

	"github.com/fr11nik/pgfx"
)

// Example демонстрирует использование pgfx как обычное взаимодействие с запросами.
func Example() {
	ctx := context.Background()

	uri := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		"user", "pass", "localhost", 5432, "db",
	)

	pg, err := pgfx.New(uri)
	checkErr(err)

	defer func() {
		checkErr(pg.Close())
	}()

	sqlStmt := `CREATE TABLE IF NOT EXISTS users_v5 (user_id SERIAL, username TEXT)`
	_, err = pg.Pool.Exec(ctx, sqlStmt)
	checkErr(err, sqlStmt)

	r := newRepo(pg.GetDBForTransactionManager())
	trManager := pg.NewTransactionManager()

	u := &user{
		Username: "username",
	}

	err = trManager.ReadCommitted(ctx, func(ctx context.Context) error {
		if err := r.Save(ctx, u); err != nil {
			return err
		}

		return trManager.ReadCommitted(ctx, func(ctx context.Context) error {
			u.Username = "new_username"

			return r.Save(ctx, u)
		})
	})
	checkErr(err)

	userFromDB, err := r.GetByID(ctx, u.ID)
	checkErr(err)

	fmt.Println(userFromDB)

	// Output: &{1 new_username}
}

type repo struct {
	db pgfx.QueryExecutor
}

func newRepo(db pgfx.QueryExecutor) *repo {
	repo := &repo{
		db: db,
	}

	return repo
}

type user struct {
	ID       int64
	Username string
}

func (r *repo) GetByID(ctx context.Context, id int64) (*user, error) {
	query := `SELECT * FROM users_v5 WHERE user_id=$1`

	conn := r.db
	row := conn.QueryRow(ctx, query, id)

	user := &user{}

	err := row.Scan(&user.ID, &user.Username)
	if err != nil {
		return nil, err
	}

	return user, nil
}

func (r *repo) Save(ctx context.Context, u *user) error {
	isNew := u.ID == 0
	conn := r.db

	if !isNew {
		query := `UPDATE users_v5 SET username = $1 WHERE user_id = $2`

		if _, err := conn.Exec(ctx, query, u.Username, u.ID); err != nil {
			return err
		}

		return nil
	}

	query := `INSERT INTO users_v5 (username) VALUES ($1) RETURNING user_id`

	err := conn.QueryRow(ctx, query, u.Username).Scan(&u.ID)
	if err != nil {
		return err
	}

	return nil
}

func checkErr(err error, args ...any) {
	if err != nil {
		panic(fmt.Sprint(append([]any{err}, args...)...))
	}
}
