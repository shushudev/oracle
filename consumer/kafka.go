package consumer

import (
	"database/sql"
	"fmt"

	"oracle/config"

	"github.com/IBM/sarama"

	"github.com/segmentio/kafka-go"

	"oracle/producer"
)

func StartMappingConsumer(db *sql.DB, writer *kafka.Writer) {
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
			HandleMappingRequest(msg.Value, db, writer)
		}
	}()
}

func StartRequestVoteMemberConsumer(db *sql.DB) {
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
			count, err := producer.FetchUserCount(db)
			if err != nil {
				fmt.Printf("[Oracle] VoteMemberCount 조회 실패: %v\n", err)
				continue
			}

			// 2. 결과 Kafka로 전송
			err = producer.PublishVoteMemberCount(count)
			if err != nil {
				fmt.Printf("[Oracle] Kafka 전송 실패: %v\n", err)
				continue
			}

			fmt.Printf("[Oracle] VoteMemberCount=%d 전송 완료\n", count)
		}
	}()
}
