// consumer/vote_reward.go
package consumer

import (
	"context"
	"database/sql"
	"log"
	"oracle/config"
	"time"
)

// Policy: R0(기준보상), Beta(최대 보너스 비율), RStart(보너스 시작 참여율) 추가
type Policy struct {
	R0             float64 // 기준 보상 (이전 BaseReward 역할)
	Beta           float64 // 최대 보너스 비율 (예: 0.5 => 최대 +50%)
	RStart         float64 // 보너스 시작 참여율 [0,1] (예: 0.5)
	UpperLimit     int     // (기존) 사용하지 않음: 과거 used 상한. 남겨두지만 보상엔 미반영.
	InactivityDays int     // 미참여 초기화 기준 일수
}

func DefaultPolicy() Policy {

	return Policy{
		R0:             config.KMAAverage,
		Beta:           0.5,
		RStart:         0.5,
		InactivityDays: 7,
		UpperLimit:     30,
	}
}

// 필요시 실제 스키마에 맞게 수정하세요.
// const userTable = "users" // 만약 테이블이 "user"라면 쌍따옴표로 감싸서 사용: FROM \"user\"

// ComputeRewards: 라운드 단위로 n, N을 먼저 구해 BaseReward를 공통 산출
func ComputeRewards(ctx context.Context, db *sql.DB, addrs []string, policy Policy) (map[string]float64, error) {
	now := time.Now().UTC()

	uniq := unique(addrs)
	n := len(uniq)

	// N: 전체 유저 수
	var N int = config.LightNodeUser
	// 주: 실제 테이블명이 "user"면 다음 쿼리를 `SELECT COUNT(*) FROM "user"`로 바꾸세요.
	//if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+userTable).Scan(&N); err != nil {
	//	return nil, err
	//}
	// 참여율 기반 BaseReward 계산 (γ=1 고정 → 선형)
	base := computeBaseRewardFromParticipation(n, N, policy)

	log.Printf("[Reward] 참여율: %.2f (n=%d, N=%d), 산출 BaseReward=%.4f, Policy={R0=%.2f, Beta=%.2f, RStart=%.2f}",
		float64(n)/float64(N), n, N, base, policy.R0, policy.Beta, policy.RStart)

	out := make(map[string]float64, len(uniq))
	for _, addr := range uniq {
		r, err := computeOne(ctx, db, addr, now, policy, base)
		if err != nil {
			return nil, err
		}
		out[addr] = r
		log.Printf("[Reward] Address=%s 지급 BaseReward=%.4f", addr, r)
	}
	return out, nil
}

// computeOne: 이전 로직 유지(카운터/last_date 관리)하되, 최종 보상은 BaseReward 그대로 반환
func computeOne(ctx context.Context, db *sql.DB, addr string, now time.Time, p Policy, baseReward float64) (float64, error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	var last sql.NullTime
	var cnt int

	// 테이블/컬럼명은 실제 스키마에 맞게 조정
	row := tx.QueryRowContext(ctx,
		`SELECT last_date, count FROM vote_counter WHERE address=$1 FOR UPDATE`, addr)
	scanErr := row.Scan(&last, &cnt)
	if scanErr == sql.ErrNoRows {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO vote_counter(address,last_date,count) VALUES($1,NULL,0)`, addr); err != nil {
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

	// 이번 투표 참여 반영
	cnt++

	// DB 갱신
	if _, err := tx.ExecContext(ctx,
		`UPDATE vote_counter SET last_date=$2, count=$3 WHERE address=$1`, addr, now, cnt); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}

	// ❗총보상 임의식 제거: 참여율 기반 BaseReward 자체를 지급
	return baseReward, nil
}

// --- 참여율 기반 BaseReward 계산 (γ=1 고정) ---

func clamp(x, lo, hi float64) float64 {
	if x < lo {
		return lo
	}
	if x > hi {
		return hi
	}
	return x
}

// r_eff = clamp((r - r_start)/(1 - r_start), 0, 1)
// bonus = Beta * r_eff^1 = Beta * r_eff
// BaseReward = R0 * (1 + bonus)
func computeBaseRewardFromParticipation(n, N int, p Policy) float64 {
	if N <= 0 || n <= 0 || p.R0 <= 0 {
		return 0
	}
	r := clamp(float64(n)/float64(N), 0, 1)

	var rEff float64
	switch {
	case p.RStart <= 0:
		rEff = r
	case p.RStart >= 1:
		// r_start가 1 이상이면, 참여율이 100%일 때만 보너스 최대
		if r >= 1 {
			rEff = 1
		} else {
			rEff = 0
		}
	default:
		rEff = clamp((r-p.RStart)/(1-p.RStart), 0, 1)
	}

	bonus := p.Beta * rEff // γ=1
	return p.R0 * (1.0 + bonus)
}

// 기존 유틸
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
