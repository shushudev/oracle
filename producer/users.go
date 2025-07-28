package producer

import (
	"fmt"
	"oracle/config"

	"github.com/IBM/sarama"
	"github.com/segmentio/kafka-go"

	"context"
	"database/sql"
	"encoding/json"
	"log"
	"time"
)

//투표자 수 최신화 관련 함수

type UserCountPayload struct {
	Count int `json:"count"`
}

// DB에서 user 수 조회
func FetchUserCount(db *sql.DB) (int, error) {
	var count int
	err := db.QueryRow(`SELECT COUNT(*) FROM userData`).Scan(&count)
	return count, err
}

// Kafka에 메시지 발행
func PublishUserCount(writer *kafka.Writer, count int) error {
	payload := UserCountPayload{Count: count}
	msgBytes, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[Users] JSON marshal error: %v", err)
		return err
	}

	err = writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte("user-count"),
		Value: msgBytes,
		Time:  time.Now(),
	})
	if err != nil {
		log.Printf("[Users] Kafka write error: %v", err)
		return err
	}

	log.Printf("[Users] Sent UserCount: %d", count)
	return nil
}

// 10초마다 user 테이블 상태 모니터링
func StartUserMonitor(db *sql.DB, writer *kafka.Writer) {
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
			err := PublishUserCount(writer, count)
			if err == nil {
				lastCount = count
			}
		}
	}
}

func PublishVoteMemberCount(count int) error {
	producer, err := sarama.NewSyncProducer(config.KafkaBrokers, nil)
	if err != nil {
		return fmt.Errorf("Kafka 프로듀서 생성 실패: %w", err)
	}
	defer producer.Close()

	msgStruct := UserCountPayload{Count: count}
	jsonBytes, err := json.Marshal(msgStruct)
	if err != nil {
		return fmt.Errorf("JSON 직렬화 실패: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: config.TopicVoteMember,
		Value: sarama.ByteEncoder(jsonBytes),
	}

	_, _, err = producer.SendMessage(msg)
	return err
}
