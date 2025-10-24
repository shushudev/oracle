// oracle/db/vote_counter.go
package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// address에 delta 점수를 누적(UPSERT) + last_time = NOW()
func UpsertVoteCounter(ctx context.Context, db *sql.DB, address string, delta float64) error {
	if address == "" {
		return fmt.Errorf("UpsertVoteCounter: empty address")
	}
	if delta == 0 {
		return nil
	}
	const q = `
INSERT INTO vote_counter (address, last_time, count)
VALUES ($1, NOW(), $2)
ON CONFLICT (address) DO UPDATE
SET last_time = NOW(),
    count = vote_counter.count + EXCLUDED.count;
`
	_, err := db.ExecContext(ctx, q, address, delta)
	return err
}

// 여러 address의 누적 점수 조회 (없으면 맵에 없음)
func GetVoteCountsByAddresses(ctx context.Context, db *sql.DB, addresses []string) (map[string]float64, error) {
	out := make(map[string]float64, len(addresses))
	if len(addresses) == 0 {
		return out, nil
	}

	var sb strings.Builder
	sb.WriteString("SELECT address, count FROM vote_counter WHERE address IN (")
	args := make([]any, 0, len(addresses))
	for i, a := range addresses {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(fmt.Sprintf("$%d", i+1))
		args = append(args, a)
	}
	sb.WriteString(")")

	rows, err := db.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var addr string
		var c float64
		if err := rows.Scan(&addr, &c); err != nil {
			return nil, err
		}
		out[addr] = c
	}
	return out, rows.Err()
}
