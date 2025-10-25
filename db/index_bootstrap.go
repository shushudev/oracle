package db

import (
	"context"
	"database/sql"
)

func EnsureFairnessIndexes(ctx context.Context, db *sql.DB, useTurnID bool) error {
	var q string
	if useTurnID {
		q = `CREATE INDEX IF NOT EXISTS idx_turn_result_turn_creator
             ON turn_result (turn_id, creator);`
	} else {
		q = `CREATE INDEX IF NOT EXISTS idx_turn_result_created_creator
             ON turn_result (created_at, creator);`
	}
	// 트랜잭션 없이 단건 실행 (CONCURRENTLY 쓸 거면 반드시 트랜잭션 없이 실행)
	_, err := db.ExecContext(ctx, q)
	return err
}
