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
	"oracle/producer"
	"oracle/types"
)

func StartVMemberRewardConsumer(db *sql.DB) error {
	log.Println("[Kafka: VMember] StartVMemberRewardConsumer")

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

			// 보상 계산
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			rewardsMap, err := ComputeRewards(ctx, db, req.Validators, DefaultPolicy())
			cancel()
			if err != nil {
				log.Printf("[VMember] 보상 계산 실패: %v", err)
				continue
			}

			// **풀노드 변경 없이 맞춘 응답 스키마**
			out := types.MemberRewardOutputMessage{
				SenderID: req.FullnodeID, // ← 중요: 풀노드 ID를 SenderID에 넣음
				Rewards:  rewardsMap,
			}
			body, err := json.Marshal(out)
			if err != nil {
				log.Printf("[VMember] 응답 직렬화 실패: %v", err)
				continue
			}

			// 전송
			msg := &sarama.ProducerMessage{
				Topic: config.TopicResultVMemberReward, // 풀노드가 듣는 토픽과 동일
				Value: sarama.ByteEncoder(body),
				// (선택) 헤더에 부가 정보
				Headers: []sarama.RecordHeader{
					{Key: []byte("fullnode_id"), Value: []byte(req.FullnodeID)},
					//{Key: []byte("request_id"), Value: []byte(req.RequestID)},
					{Key: []byte("ts"), Value: []byte(time.Now().UTC().Format(time.RFC3339))},
				},
			}
			if _, _, err := producer.RewardProducer.SendMessage(msg); err != nil {
				log.Printf("[VMember] 응답 전송 실패: %v", err)
				continue
			}

			log.Printf("[VMember] 보상 응답 전송 완료: 대상=%d", len(rewardsMap))
		}
	}()
	return nil
}
