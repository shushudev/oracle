package producer

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"oracle/config"
	"oracle/types"
	"time"

	"github.com/IBM/sarama"
)

// DB에서 user 수 조회
func FetchUserCount(db *sql.DB) (int, error) {
	var count int
	err := db.QueryRow(`SELECT COUNT(*) FROM userData`).Scan(&count)
	return count, err
}

// Kafka에 메시지 발행 (Sarama)
func PublishUserCount(writer sarama.SyncProducer, count int) error {
	payload := types.UserCountPayload{Count: count}
	msgBytes, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[Users] JSON marshal error: %v", err)
		return err
	}

	message := &sarama.ProducerMessage{
		Topic: config.TopicVoteMemberProducer,
		Key:   sarama.ByteEncoder([]byte("user-count")),
		Value: sarama.ByteEncoder(msgBytes),
	}

	partition, offset, err := writer.SendMessage(message)
	if err != nil {
		log.Printf("[Users] Kafka write error: %v", err)
		return err
	}

	log.Printf("[Users] Sent UserCount: %d (partition=%d, offset=%d)", count, partition, offset)
	return nil
}

// 10초마다 user 테이블 상태 모니터링
func StartUserMonitor(db *sql.DB, producer sarama.SyncProducer) {
	log.Println("[Users] User DB polling monitor started...")
	var lastCount int

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		count, err := FetchUserCount(db)
		if err != nil {
			log.Printf("[Users] DB query error: %v", err)
			continue
		}

		if count != lastCount {
			err := PublishUserCount(producer, count)
			if err == nil {
				lastCount = count
				config.LightNodeUser = lastCount
			}
		}
	}
}

func StartRequestVoteMemberConsumer(db *sql.DB, writer sarama.SyncProducer) {
	fmt.Println("[Oracle] StartRequestVoteMemberConsumer 시작됨")

	brokers := config.KafkaBrokers
	topic := config.TopicRequestMemberCount
	partition := int32(0)

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_1_0_0

	consumer, err := sarama.NewConsumer(brokers, saramaConfig)
	if err != nil {
		panic(fmt.Sprintf("[Oracle] Kafka Consumer 생성 실패: %v", err))
	}

	partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
	if err != nil {
		panic(fmt.Sprintf("[Oracle] 파티션 구독 실패: %v", err))
	}

	go func() {
		fmt.Println("[Oracle] Kafka 요청 수신 대기 중...")
		for msg := range partitionConsumer.Messages() {
			fmt.Printf("[Oracle] 수신 요청 메시지: %s\n", string(msg.Value))

			// 1. DB에서 현재 투표 수 조회
			count, err := FetchUserCount(db)
			if err != nil {
				fmt.Printf("[Oracle] VoteMemberCount 조회 실패: %v\n", err)
				continue
			}

			// 2. 결과 Kafka로 전송
			err = PublishUserCount(writer, count)
			fmt.Printf("[Oracle] Kafka 전송 실패: %v\n", err)
			if err != nil {
				continue
			}

			fmt.Printf("[Oracle] VoteMemberCount=%d 전송 완료\n", count)
		}
	}()
}
