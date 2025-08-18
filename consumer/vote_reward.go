package consumer

type RewardPolicy struct {
	BaseReward     float64 // 기본 보상 (coin)
	UpperLimit     int     // 참여횟수 상한
	InactivityDays int     // 미참여 초기화 기준 일수 (예: 7)
}

type RewardResult struct {
	UserID         int64
	AfterCount     int // 이번 반영 후 vote_count
	UsedCount      int // 보상 계산에 사용된 카운트(min(count, upper))
	BaseReward     float64
	VariableReward float64
	TotalReward    float64
}

// 보상식: 기본 보상금 + 참여횟수 X 0.1 X 기본 보상금
/*
	count : 투표 횟수
	upper : 투표 횟수 상한선
	used : 계산에 반영할 최종 투표 횟수
*/
func calcReward(base float64, count, upper int) (used int, variable, total float64) {
	if count < upper {
		used = count
	} else {
		used = upper
	}
	variable = float64(used) * (0.1 * base)
	total = base + variable
	return
}


// 한 건의 투표를 반영하고 보상을 계산한다.
// 테이블: vote_counters(user_id PK, latest_vote_time TIMESTAMPTZ, vote_count INT)
func UpdateVoteAndComputeReward(
	ctx context.Context,
	db *sql.DB,
	userID int64,
	votedAt time.Time,     // 이번 투표 시각
	policy RewardPolicy,   // {BaseReward, UpperLimit, InactivityDays}
) (*RewardResult, error) {

	// 직렬화 수준 권장(경쟁 조건 방지)
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	// 1) 없는 사용자 row 생성(UPSERT) - 최초 참여 대비
	_, err = tx.ExecContext(ctx, `
		INSERT INTO vote_counters (user_id, latest_vote_time, vote_count)
		VALUES ($1, NULL, 0)
		ON CONFLICT (user_id) DO NOTHING
	`, userID)
	if err != nil {
		return nil, err
	}

	// 2) 행 잠금 후 현재 상태 조회
	var (
		curLatest sql.NullTime
		curCount  int
	)
	err = tx.QueryRowContext(ctx, `
		SELECT latest_vote_time, vote_count
		FROM vote_counters
		WHERE user_id = $1
		FOR UPDATE
	`, userID).Scan(&curLatest, &curCount)
	if err != nil {
		return nil, err
	}

	// 3) 미참여 초기화 규칙
	if curLatest.Valid {
		inactive := votedAt.Sub(curLatest.Time)
		if inactive >= (time.Duration(policy.InactivityDays) * 24 * time.Hour) {
			curCount = 0 // 초기화
		}
	}

	// 4) 이번 참여 반영
	curCount += 1
	_, err = tx.ExecContext(ctx, `
		UPDATE vote_counters
		SET latest_vote_time = $2,
		    vote_count       = $3
		WHERE user_id = $1
	`, userID, votedAt, curCount)
	if err != nil {
		return nil, err
	}

	// 5) 보상 계산
	used, variable, total := calcReward(policy.BaseReward, curCount, policy.UpperLimit)

	// (선택) 보상 원장에 기록하려면 아래와 같은 테이블을 두고 INSERT 하세요.
	// reward_ledger(user_id, base_reward, variable_reward, total_reward, vote_count_used, calc_at)
	// _, _ = tx.ExecContext(ctx, `
	// 	INSERT INTO reward_ledger
	// 	  (user_id, base_reward, variable_reward, total_reward, vote_count_used, calc_at)
	// 	VALUES ($1, $2, $3, $4, $5, now())
	// `, userID, policy.BaseReward, variable, total, used)

	if err = tx.Commit(); err != nil {
		return nil, err
	}

	return &RewardResult{
		UserID:         userID,
		AfterCount:     curCount,
		UsedCount:      used,
		BaseReward:     policy.BaseReward,
		VariableReward: variable,
		TotalReward:    total,
	}, nil
}