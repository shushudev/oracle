package consumer

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"oracle/config"
	"strconv"

	"github.com/IBM/sarama"
)

type Contributor struct {
	Address   string `json:"address,omitempty"`
	EnergyKwh string `json:"energy_kwh,omitempty"`
}

// Kafka 메시지 구조 (Producer에서 보낸 형식)
type BlockContributorMsg struct {
	FullnodeID   string        `json:"fullnode_id"`
	Contributors []Contributor `json:"contributors"`
}

type BlockCreatorMsg struct {
	Creator      string  `json:"creator"`
	Contribution float64 `json:"contribution"`
	FullnodeID   string  `json:"fullnode_id"`
}

func StartBlockCreatorConsumer(db *sql.DB, producer sarama.SyncProducer) {

	brokers := config.KafkaBrokers
	topic := config.TopicContributors
	partition := int32(0)

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_1_0_0

	consumer, err := sarama.NewConsumer(brokers, saramaConfig)
	if err != nil {
		panic(fmt.Sprintf("[Kafka: Contributor] Consumer 생성 실패: %v", err))
	}

	partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
	if err != nil {
		panic(fmt.Sprintf("[Kafka: Contributor] 파티션 구독 실패: %v", err))
	}

	go func() {
		fmt.Println("[Kafka: Contributor] Partition Consumer 수신 대기 중...")
		for msg := range partitionConsumer.Messages() {

			// === JSON 언마샬링 ===
			var data BlockContributorMsg
			err := json.Unmarshal(msg.Value, &data)
			if err != nil {
				fmt.Printf("[Kafka: Contributor] JSON 언마샬링 실패: %v\n", err)
				continue
			}

			// === 수신 데이터 출력 ===
			fmt.Printf("[Kafka: Contributor] FullnodeID: %s | Contributors 수: %d\n",
				data.FullnodeID, len(data.Contributors))

			for _, c := range data.Contributors {
				fmt.Printf("  - Address: %s | EnergyKwh: %s\n", c.Address, c.EnergyKwh)
			}

			// === BlockCreatorMsg 생성 (첫 번째 contributor만 사용) ===
			if len(data.Contributors) > 0 {
				first := data.Contributors[0]

				// energy_kwh를 float 변환 (혹시 필요할 경우 대비)
				_, _ = strconv.ParseFloat(first.EnergyKwh, 64) // 실제로는 사용 안 하지만 변환 체크

				blockCreator := BlockCreatorMsg{
					Creator:      first.Address,   // 첫 번째 contributor의 address
					Contribution: 50,              // 고정값 50
					FullnodeID:   data.FullnodeID, // fullnode_id 그대로
				}

				// JSON 직렬화
				payload, err := json.Marshal(blockCreator)
				if err != nil {
					fmt.Printf("[Kafka: Contributor] BlockCreatorMsg 직렬화 실패: %v\n", err)
					continue
				}

				// Kafka 메시지 전송
				produceMsg := &sarama.ProducerMessage{
					Topic: config.TopicBlockCreator, // 새 토픽 지정
					Value: sarama.ByteEncoder(payload),
				}

				_, _, err = producer.SendMessage(produceMsg)
				if err != nil {
					fmt.Printf("[Kafka: Contributor] BlockCreatorMsg 전송 실패: %v\n", err)
				} else {
					fmt.Printf("[Kafka: Contributor] BlockCreatorMsg 전송 완료 → Creator=%s, FullnodeID=%s\n",
						blockCreator.Creator, blockCreator.FullnodeID)
				}
			}
		}
	}()
}
