// oracle/reward/reward.go
package consumer

import (
	"context"
	"database/sql"
	"time"
)

type Policy struct {
	BaseReward     float64 // 기본 보상
	UpperLimit     int     // 상한
	InactivityDays int     // 미참여 초기화 기준 일수
}

func DefaultPolicy() Policy {
	return Policy{BaseReward: 1.0, UpperLimit: 30, InactivityDays: 7}
}

func ComputeRewards(ctx context.Context, db *sql.DB, addrs []string, policy Policy) (map[string]float64, error) {
	now := time.Now().UTC()
	out := make(map[string]float64, len(addrs))
	for _, addr := range unique(addrs) {
		r, err := computeOne(ctx, db, addr, now, policy)
		if err != nil {
			return nil, err
		}
		out[addr] = r
	}
	return out, nil
}

func computeOne(ctx context.Context, db *sql.DB, addr string, now time.Time, p Policy) (float64, error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	var last sql.NullTime
	var cnt int
	// 테이블명/컬럼명은 실제 스키마에 맞춰 변경
	row := tx.QueryRowContext(ctx, `SELECT last_date, count FROM vote_counter WHERE address=$1 FOR UPDATE`, addr)
	scanErr := row.Scan(&last, &cnt)
	if scanErr == sql.ErrNoRows {
		if _, err := tx.ExecContext(ctx, `INSERT INTO vote_counter(address,last_date,count) VALUES($1,NULL,0)`, addr); err != nil {
			return 0, err
		}
		cnt = 0
		last.Valid = false
	} else if scanErr != nil {
		return 0, scanErr
	}

	// 미참여 초기화
	if last.Valid && now.Sub(last.Time) >= time.Duration(p.InactivityDays)*24*time.Hour {
		cnt = 0
	}

	// 이번 참여 반영
	cnt++

	used := cnt
	if used > p.UpperLimit {
		used = p.UpperLimit
	}

	total := p.BaseReward + float64(used)*0.1*p.BaseReward

	if _, err := tx.ExecContext(ctx, `UPDATE vote_counter SET last_date=$2, count=$3 WHERE address=$1`, addr, now, cnt); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return total, nil
}

func unique(in []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
