package consumer

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"oracle/config"

	"github.com/IBM/sarama"
	"github.com/lib/pq"
)

type TxHashResult struct {
	Address string `json:"address"`
	Hash    string `json:"hash"`
}

func StartTxHashConsumer(db *sql.DB) {

	brokers := config.KafkaBrokers
	topic := config.TopicTxHash
	partition := int32(0)

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_1_0_0

	consumer, err := sarama.NewConsumer(brokers, saramaConfig)
	if err != nil {
		panic(fmt.Sprintf("[Kafka: TxHash] Consumer 생성 실패: %v", err))
	}

	partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
	if err != nil {
		panic(fmt.Sprintf("[Kafka: TxHash] 파티션 구독 실패: %v", err))
	}

	go func() {
		fmt.Println("[Kafka: TxHash] Partition Consumer 수신 대기 중...")
		for msg := range partitionConsumer.Messages() {
			fmt.Printf("[Kafka: TxHash] 수신 메시지: %s\n", string(msg.Value))

			// 메시지를 처리하는 기존 로직 호출
			HandleQuery(db, msg.Value)
		}
	}()
}
func HandleQuery(db *sql.DB, msgValue []byte) {
	// 1. Unmarshal the JSON message
	var result TxHashResult
	err := json.Unmarshal(msgValue, &result)
	if err != nil {
		log.Printf("[Kafka: TxHash] JSON Unmarshal 실패: %v", err)
		return
	}

	// 2. Insert into the database
	// The SQL statement to insert a new row
	query := `INSERT INTO solar_archive (address, hash) VALUES ($1, $2)`

	// Execute the query using the database connection pool
	_, err = db.Exec(query, result.Address, result.Hash)
	if err != nil {
		log.Printf("[Kafka: TxHash] DB Insert 실패 (Address: %s, Hash: %s): %v", result.Address, result.Hash, err)
		// Check for unique constraint violation specifically
		// (This part is database driver-specific, e.g., for PostgreSQL)
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23505" {
			log.Printf("중복된 해시값 (hash: %s)입니다. 건너뜁니다.", result.Hash)
			return
		}
		return
	}

	log.Printf("[Kafka: TxHash] DB에 성공적으로 기록: Address=%s, Hash=%s", result.Address, result.Hash)
}
