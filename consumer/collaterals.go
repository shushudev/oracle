package consumer

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"oracle/config"
	"oracle/types"
	"strconv"

	"github.com/IBM/sarama"

	"github.com/segmentio/kafka-go"
)

func StartCollateralsConsumer(db *sql.DB, writer *kafka.Writer) {
	fmt.Println("[Kafka: Collaterals] StartCollateralsConsumer 시작됨")

	brokers := config.KafkaBrokers
	topic := config.TopicCollateralsResistration
	partition := int32(0)

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_1_0_0

	consumer, err := sarama.NewConsumer(brokers, saramaConfig)
	if err != nil {
		panic(fmt.Sprintf("[Kafka: Collaterals] Consumer 생성 실패: %v", err))
	}

	partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
	if err != nil {
		panic(fmt.Sprintf("[Kafka: Collaterals] 파티션 구독 실패: %v", err))
	}

	go func() {
		fmt.Println("[Kafka: Collaterals] Partition Consumer 수신 대기 중...")
		for msg := range partitionConsumer.Messages() {
			fmt.Printf("[Kafka: Collaterals] 수신 메시지: %s\n", string(msg.Value))

			// 메시지를 처리하는 기존 로직 호출
			HandleCollaterals(msg.Value, db, writer)
		}
	}()
}

func HandleCollaterals(msg []byte, db *sql.DB, writer *kafka.Writer) {
	var data types.RECMeta
	if err := json.Unmarshal(msg, &data); err != nil {
		log.Printf("[Collaterals] JSON decode error: %v\n", err)
		return
	}

	// === 1. INSERT ===
	query := `
		INSERT INTO Collaterals (
			facility_id, facility_name, location, technology_type, capacity_mw,
			registration_date, certified_id, issue_data,
			generation_start_date, generation_end_date, measured_volume_mwh,
			retired_date, retirement_purpose, status, timestamp
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
	`
	_, err := db.Exec(query,
		data.FacilityID,
		data.FacilityName,
		data.Location,
		data.TechnologyType,
		data.CapacityMW,
		data.RegistrationDate,
		data.CertifiedId,
		data.IssueDate,
		data.GenerationStartDate,
		data.GenerationEndDate,
		data.MeasuredVolumeMWh,
		data.RetiredDate,
		data.RetirementPurpose,
		data.Status,
		data.Timestamp,
	)
	if err != nil {
		log.Printf("[Collaterals] DB Insert 실패: %v\n", err)
		return
	}
	log.Printf("[Collaterals] DB Insert 성공: FacilityID=%s, CertifiedID=%s", data.FacilityID, data.CertifiedId)

	// === 2. DB 총 개수 조회 ===
	var dbCount int
	err = db.QueryRow("SELECT COUNT(*) FROM Collaterals").Scan(&dbCount)
	if err != nil {
		log.Printf("[Collaterals] DB Count 조회 실패: %v\n", err)
		return
	}
	log.Printf("[Collaterals] 현재 DB 총 개수: %d", dbCount)

	// === 3. REST API 호출 ===
	resp, err := http.Get("http://192.168.0.19:1317/cosmos/reward/v1beta1/collateral")
	if err != nil {
		log.Printf("[Collaterals] REST API 호출 실패: %v\n", err)
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var apiResp struct {
		TotalAmount string `json:"total_amount"`
	}
	if err := json.Unmarshal(body, &apiResp); err != nil {
		log.Printf("[Collaterals] API 응답 파싱 실패: %v\n", err)
		return
	}

	totalAmount, _ := strconv.Atoi(apiResp.TotalAmount)
	log.Printf("[Collaterals] REST API total_amount=%d", totalAmount)

	// === 4. 비교 후 Kafka 전송 ===
	if dbCount > totalAmount {
		// 차이만큼 예치 요청
		diff := dbCount - totalAmount
		alert := types.CollateralMessage{
			REC: fmt.Sprintf("%d", diff),
		}

		encoded, err := json.Marshal(alert)
		if err != nil {
			log.Printf("[Collaterals] JSON 직렬화 실패: %v\n", err)
			return
		}

		producerMsg := kafka.Message{
			Value: encoded,
		}

		if err := writer.WriteMessages(nil, producerMsg); err != nil {
			log.Printf("[Collaterals] Kafka 전송 실패: %v\n", err)
		} else {
			log.Printf("[Collaterals] Kafka 담보 예치 메시지 전송 완료: %s", string(encoded))
		}
	}
}
