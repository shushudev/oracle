// oracle/consumer/vmember_reward_consumer.go
package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"time"

	"github.com/IBM/sarama"

	"oracle/config"
	dbx "oracle/db"
	"oracle/types"
)

func StartVMemberRewardConsumer(db *sql.DB /* writer 제거됨 */) error {
	{
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		err := SaveSolarRadiationJSON(ctx)
		cancel()
		if err != nil {
			log.Printf("[WARN] initial KMA average load failed: %v", err)
		} else {
			log.Printf("[R0] Base vote Reward : %.6f", config.R_0)
		}
	}
	StartSolarAverageScheduler(context.Background())

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_1_0_0

	cons, err := sarama.NewConsumer(config.KafkaBrokers, cfg)
	if err != nil {
		return err
	}

	const partition = int32(0)
	pc, err := cons.ConsumePartition(config.TopicRequestVMemberReward, partition, sarama.OffsetNewest)
	if err != nil {
		_ = cons.Close()
		return err
	}

	go func() {
		defer func() { _ = pc.Close(); _ = cons.Close() }()

		for m := range pc.Messages() {
			if m == nil || len(m.Value) == 0 {
				continue
			}

			var req types.VMemberRequestMessage
			if err := json.Unmarshal(m.Value, &req); err != nil {
				log.Printf("[VMember] 요청 파싱 실패: %v", err)
				continue
			}

			// 보상 계산 (올바른 인자 사용)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			rewardsMap, err := ComputeRewards(ctx, db, req.Validators, DefaultPolicy())
			cancel()
			if err != nil {
				log.Printf("[VMember] 보상 계산 실패: %v", err)
				continue
			}

			// 송금/응답 전송 대신 vote_counter에 점수 누적
			upserted := 0
			for addr, score := range rewardsMap {
				if score <= 0 {
					continue
				}
				if err := dbx.UpsertVoteCounter(context.Background(), db, addr, score); err != nil {
					log.Printf("[VMember] vote_counter upsert 실패 addr=%s score=%.8f err=%v", addr, score, err)
					continue
				}
				upserted++
			}

			log.Printf("[VMember] 보상 누적 완료: 대상=%d, fullnode_id=%s, ts=%s",
				upserted, req.FullnodeID, time.Now().UTC().Format(time.RFC3339))
		}
	}()
	return nil
}
