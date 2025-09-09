package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"oracle/config"

	"github.com/IBM/sarama"

	"github.com/segmentio/kafka-go"

	"oracle/types"
)

var alpha = 0.5 // 상대비교 가중치

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
			var payload types.LocationPayload
			if err := json.Unmarshal(msg.Value, &payload); err != nil {
				fmt.Printf("[Kafka: Location] 메시지 파싱 실패: %v\n", err)
				continue
			}

			fmt.Printf("[Kafka: Location] 받은 해시: %s | 위도: %f, 경도: %f\n", payload.Hash, payload.Location.Latitude, payload.Location.Longitutde)

			plants, err := LoadAllNuclearPlants(db)
			if err != nil {
				log.Fatalf("DB에서 발전소 목록 불러오기 실패: %v", err)
			}

			closestPlant, distance := FindClosestPlant(plants, payload.Location.Latitude, payload.Location.Longitutde)
			fmt.Printf("가장 가까운 발전소: %s (%.2f km)\n", closestPlant.PlantName, distance)

			reward := calcDistWeight(distance)
			fmt.Printf("거리 기반 보상 가중치: %.2f\n", reward)

			pop, err := GetPopulationByLatLon(payload.Location.Latitude, payload.Location.Longitutde, "2023")
			if err != nil {
				fmt.Println("🚫 오류:", err)
			} else {
				fmt.Printf("위치 인구 수: %d명\n", pop)

				// 인구 기반 가중치
				popWeight := calcPopulationRewardWeight(pop)
				fmt.Printf("인구 기반 보상 가중치: %.2f\n", popWeight)
				reward = reward*alpha + popWeight*(1-alpha)
			}

			// Kafka로 결과 전송
			output := types.LocationOutputMessage{
				Hash:     payload.Hash,
				Output:   reward,
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
				fmt.Printf("[Kafka: Location] 가중치 전송 실패 : %v\n", err)
			} else {
				fmt.Printf("[Kafka: Location] 가중치 전송 완료: %s → %f\n", payload.Hash, reward)
			}
		}
	}()
}

func LoadAllNuclearPlants(db *sql.DB) ([]types.Plant, error) {
	query := `
		SELECT 
			id, plant_name, region, city, town, latitude, longitude
		FROM nuclear_power_plants
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("원자력 발전소 정보 조회 실패: %w", err)
	}
	defer rows.Close()

	var plants []types.Plant
	for rows.Next() {
		var p types.Plant
		err := rows.Scan(
			&p.ID,
			&p.PlantName,
			&p.Region,
			&p.City,
			&p.Town,
			&p.Latitude,
			&p.Longitude,
		)
		if err != nil {
			return nil, fmt.Errorf("행 데이터 스캔 실패: %w", err)
		}
		plants = append(plants, p)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("결과 순회 중 오류: %w", err)
	}

	return plants, nil
}

// 두 좌표 사이 거리 (단위: km)
func haversine(lat1, lon1, lat2, lon2 float64) float64 {
	const R = 6371 // 지구 반지름 (킬로미터)
	dLat := (lat2 - lat1) * math.Pi / 180
	dLon := (lon2 - lon1) * math.Pi / 180
	lat1Rad := lat1 * math.Pi / 180
	lat2Rad := lat2 * math.Pi / 180

	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1Rad)*math.Cos(lat2Rad)*
			math.Sin(dLon/2)*math.Sin(dLon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return R * c
}

func FindClosestPlant(plants []types.Plant, targetLat, targetLon float64) (*types.Plant, float64) {
	var closest *types.Plant
	minDistance := math.MaxFloat64

	for _, plant := range plants {
		dist := haversine(targetLat, targetLon, plant.Latitude, plant.Longitude)
		if dist < minDistance {
			minDistance = dist
			closest = &plant
		}
	}

	return closest, minDistance
}

// 거리(km)를 입력받아 보상 가중치를 계산
const MaxDistance = 325.0 // 정책에 따라 조정
var MaxPop = 568000.0     // 가장 인구수가 많은 지역 (노원구)

func calcDistWeight(distanceKm float64) float64 {
	if distanceKm <= 0 {
		return 0.0
	}
	weight := distanceKm / MaxDistance
	if weight > 1.0 {
		weight = 1.0
	}
	return weight
}

func calcPopulationRewardWeight(pop int) float64 {
	if pop <= 0 {
		return 0.0
	}
	weight := math.Log(float64(pop)) / math.Log(MaxPop)
	if weight > 1.0 {
		return 1.0
	}
	return weight
}
