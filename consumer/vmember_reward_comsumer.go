// oracle/consumer/vmember_reward_consumer.go
package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/segmentio/kafka-go"

	"oracle/config"
	"oracle/types"
)

func StartVMemberRewardConsumer(db *sql.DB, writer *kafka.Writer) error {
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

			out := types.MemberRewardOutputMessage{
				SenderID: req.FullnodeID,
				Rewards:  rewardsMap,
			}

			body, err := json.Marshal(out)
			if err != nil {
				log.Printf("[VMember] 응답 직렬화 실패: %v", err)
				continue
			}

			// writer로 Kafka 메시지 전송
			err = writer.WriteMessages(
				context.Background(),
				kafka.Message{
					Value: body,
					Headers: []kafka.Header{
						{Key: "fullnode_id", Value: []byte(req.FullnodeID)},
						{Key: "ts", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
					},
				},
			)
			if err != nil {
				log.Printf("[VMember] 응답 전송 실패: %v", err)
				continue
			}

			log.Printf("[VMember] 보상 응답 전송 완료: 대상=%d", len(rewardsMap))

		}
	}()
	return nil
}
