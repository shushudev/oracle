package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

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

type Location struct {
	Latitude   float64 `json:"latitude"`
	Longitutde float64 `json:"longitude"` // ← 오타 주의: Longitutde → Longitude
}

type LocationPayload struct {
	Hash     string   `json:"hash"`
	Location Location `json:"location"`
	SenderID string   `json:"sender_id"`
}

type LocationOutputMessage struct {
	Hash     string `json:"hash"`
	Output   string `json:"output"`
	SenderID string `json:"sender_id"`
}

func StartLocationConsumer(db *sql.DB, writer *kafka.Writer) {
	fmt.Println("[Kafka: Location] Start Location Consumer")

	brokers := config.KafkaBrokers
	topic := config.TopicRequestLocation
	partition := int32(0)

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_1_0_0

	consumer, err := sarama.NewConsumer(brokers, saramaConfig)
	if err != nil {
		panic(fmt.Sprintf("[Kafka: Location] Consumer 생성 실패: %v", err))
	}

	partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
	if err != nil {
		panic(fmt.Sprintf("[Kafka: Location] 파티션 구독 실패: %v", err))
	}

	go func() {
		fmt.Println("[Kafka: Location] Partition Consumer 수신 대기 중...")
		for msg := range partitionConsumer.Messages() {
			var payload LocationPayload
			if err := json.Unmarshal(msg.Value, &payload); err != nil {
				fmt.Printf("[Kafka: Location] 메시지 파싱 실패: %v\n", err)
				continue
			}

			fmt.Printf("[Kafka: Location] 받은 해시: %s | 위도: %f, 경도: %f\n", payload.Hash, payload.Location.Latitude, payload.Location.Longitutde)

			// 역지오코딩 API 호출
			regionName := getRegionName(payload.Location.Latitude, payload.Location.Longitutde)
			if regionName == "" {
				fmt.Println("⚠️ 지역명 조회 실패. Kafka 전송 생략")
				continue
			}

			// Kafka로 결과 전송
			output := LocationOutputMessage{
				Hash:     payload.Hash,
				Output:   regionName,
				SenderID: payload.SenderID,
			}

			outputBytes, _ := json.Marshal(output)
			err = writer.WriteMessages(
				context.Background(),
				kafka.Message{
					Value: outputBytes,
				},
			)
			if err != nil {
				fmt.Printf("[Kafka: Location] 지역명 Kafka 전송 실패: %v\n", err)
			} else {
				fmt.Printf("[Kafka: Location] 지역명 전송 완료: %s → %s\n", payload.Hash, regionName)
			}
		}
	}()
}

// 역지오코딩: 위도/경도 → 지역명 (OpenStreetMap Nominatim API 사용)
func getRegionName(lat, lng float64) string {
	url := fmt.Sprintf("https://nominatim.openstreetmap.org/reverse?format=json&lat=%f&lon=%f", lat, lng)

	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("User-Agent", "capstone-location-resolver")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("❌ 역지오코딩 API 호출 실패:", err)
		return ""
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)

	var result struct {
		DisplayName string `json:"display_name"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		fmt.Println("❌ 역지오코딩 결과 파싱 실패:", err)
		return ""
	}
	return result.DisplayName
}
