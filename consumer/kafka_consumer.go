package consumer

import (
	"database/sql"
	"fmt"
	"oracle/config"

	"github.com/IBM/sarama"
)

func StartMappingConsumer(db *sql.DB, producer sarama.SyncProducer) {
	fmt.Println("[Kafka: Mapping] StartMappingConsumer 시작됨")

	brokers := config.KafkaBrokers
	topic := config.TopicDeviceIdToAddressRequest
	partition := int32(0)

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_1_0_0

	consumer, err := sarama.NewConsumer(brokers, saramaConfig)
	if err != nil {
		panic(fmt.Sprintf("[Kafka: Mapping] Consumer 생성 실패: %v", err))
	}

	partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
	if err != nil {
		panic(fmt.Sprintf("[Kafka: Mapping] 파티션 구독 실패: %v", err))
	}

	go func() {
		fmt.Println("[Kafka: Mapping] Partition Consumer 수신 대기 중...")
		for msg := range partitionConsumer.Messages() {
			fmt.Printf("[Kafka: Mapping] 수신 메시지: %s\n", string(msg.Value))

			// 메시지를 처리하는 기존 로직 호출
			HandleMappingRequest(msg.Value, db, producer)
		}
	}()
}
