package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// 앱 시작 시 1회 호출하여 테이블 보장
func EnsureTurnTables(ctx context.Context, db *sql.DB) error {
	const ddl = `
CREATE TABLE IF NOT EXISTS turn_result (
  turn_id     TEXT PRIMARY KEY,
  fullnode_id TEXT NOT NULL,
  creator     TEXT NOT NULL,
  weight      DOUBLE PRECISION NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS vote_counter_ledger (
  turn_id           TEXT NOT NULL,
  address           TEXT NOT NULL,
  before_count      DOUBLE PRECISION NOT NULL,
  after_count       DOUBLE PRECISION NOT NULL,
  delta             DOUBLE PRECISION NOT NULL,
  before_last_time  TIMESTAMPTZ,
  after_last_time   TIMESTAMPTZ,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (turn_id, address)
);
CREATE INDEX IF NOT EXISTS idx_vcl_turn ON vote_counter_ledger (turn_id);
CREATE INDEX IF NOT EXISTS idx_vcl_addr ON vote_counter_ledger (address);`
	_, err := db.ExecContext(ctx, ddl)
	return err
}

// VALUES ($1),($2),... 동적 생성
func buildValuesPlaceholders(addrs []string, start int) (string, []any) {
	var b strings.Builder
	args := make([]any, 0, len(addrs))
	for i, a := range addrs {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "($%d)", start+i)
		args = append(args, a)
	}
	return b.String(), args
}

// 이번 턴 "합집합 후보만" reset + ledger 기록 + idempotency
// - turnID: 이번 턴 고유 식별자(consumer의 seedMaterial 사용 권장)
// - addrs : 합집합 후보 주소 배열(중복 없는 상태로 넘겨주세요)
func FinalizeTurnSubsetTx(
	ctx context.Context, db *sql.DB,
	turnID, fullnodeID, winner string, weight float64, addrs []string,
) (err error) {

	if len(addrs) == 0 {
		return nil
	}

	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	// 1) 동일 turn_id 병행 보호
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, turnID); err != nil {
		return err
	}

	// 2) idempotency: turn_result 선삽입(이미 있으면 스킵)
	res, err := tx.ExecContext(ctx,
		`INSERT INTO turn_result (turn_id, fullnode_id, creator, weight)
		 VALUES ($1,$2,$3,$4)
		 ON CONFLICT (turn_id) DO NOTHING`,
		turnID, fullnodeID, winner, weight)
	if err != nil {
		return err
	}
	if aff, _ := res.RowsAffected(); aff == 0 {
		return nil // 이미 처리된 턴
	}

	// 3) 후보만 reset + ledger 기록(last_time 포함)
	vals, args := buildValuesPlaceholders(addrs, 1)
	q := fmt.Sprintf(`
WITH cand(address) AS (VALUES %s),
sel AS (
  SELECT v.address, v.count AS before_count, v.last_time AS before_last_time
  FROM vote_counter v
  JOIN cand c ON c.address = v.address
  WHERE v.count <> 0
  FOR UPDATE
),
upd AS (
  UPDATE vote_counter v
     SET count = 0,
         last_time = now()   -- 리셋 시각 반영(원치 않으면 이 줄 제거)
    FROM sel s
   WHERE v.address = s.address
RETURNING v.address, s.before_count, s.before_last_time, v.last_time AS after_last_time
)
INSERT INTO vote_counter_ledger
(turn_id, address, before_count, after_count, delta, before_last_time, after_last_time)
SELECT $%d, u.address, u.before_count, 0, (0 - u.before_count), u.before_last_time, u.after_last_time
  FROM upd u;
`, vals, len(args)+1)

	args = append(args, turnID)
	if _, err = tx.ExecContext(ctx, q, args...); err != nil {
		return err
	}
	return nil
}

// 전체 reset + ledger 기록
func FinalizeTurnAllTx(
	ctx context.Context, db *sql.DB,
	turnID, fullnodeID, winner string, weight float64,
) (err error) {

	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, turnID); err != nil {
		return err
	}

	res, err := tx.ExecContext(ctx,
		`INSERT INTO turn_result (turn_id, fullnode_id, creator, weight)
		 VALUES ($1,$2,$3,$4)
		 ON CONFLICT (turn_id) DO NOTHING`,
		turnID, fullnodeID, winner, weight)
	if err != nil {
		return err
	}
	if aff, _ := res.RowsAffected(); aff == 0 {
		return nil
	}

	const q = `
WITH sel AS (
  SELECT address, count AS before_count, last_time AS before_last_time
    FROM vote_counter
   WHERE count <> 0
   FOR UPDATE
),
upd AS (
  UPDATE vote_counter v
     SET count = 0,
         last_time = now()
    FROM sel s
   WHERE v.address = s.address
RETURNING v.address, s.before_count, s.before_last_time, v.last_time AS after_last_time
)
INSERT INTO vote_counter_ledger
(turn_id, address, before_count, after_count, delta, before_last_time, after_last_time)
SELECT $1, u.address, u.before_count, 0, (0 - u.before_count), u.before_last_time, u.after_last_time
  FROM upd u;`
	_, err = tx.ExecContext(ctx, q, turnID)
	return err
}
