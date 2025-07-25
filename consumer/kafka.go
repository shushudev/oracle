package consumer

import (
	"context"
	"log"
	"time"
	"database/sql"

	"github.com/segmentio/kafka-go"
	"oracle/config"
)

func StartMappingConsumer(db *sql.DB, writer *kafka.Writer) {
	brokers := config.KafkaBrokers
	topic := config.TopicDeviceIdToAddress
	groupID := config.GroupDeviceIdToAddress

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     groupID,
		StartOffset: kafka.LastOffset,
		MaxWait:     1 * time.Second,
	})

	log.Println("[Mapping] Oracle Kafka Consumer started...")

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Printf("[Mapping] Kafka read error: %v\n", err)
			continue
		}
		HandleMappingRequest(m.Value, db, writer)
	}
}
