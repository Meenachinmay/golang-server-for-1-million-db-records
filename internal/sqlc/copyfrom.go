// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.25.0
// source: copyfrom.go

package sqlc

import (
	"context"
)

// iteratorForCreateUser implements pgx.CopyFromSource.
type iteratorForCreateUser struct {
	rows                 []string
	skippedFirstNextCall bool
}

func (r *iteratorForCreateUser) Next() bool {
	if len(r.rows) == 0 {
		return false
	}
	if !r.skippedFirstNextCall {
		r.skippedFirstNextCall = true
		return true
	}
	r.rows = r.rows[1:]
	return len(r.rows) > 0
}

func (r iteratorForCreateUser) Values() ([]interface{}, error) {
	return []interface{}{
		r.rows[0],
	}, nil
}

func (r iteratorForCreateUser) Err() error {
	return nil
}

func (q *Queries) CreateUser(ctx context.Context, name []string) (int64, error) {
	return q.db.CopyFrom(ctx, []string{"users"}, []string{"name"}, &iteratorForCreateUser{rows: name})
}