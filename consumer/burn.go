package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"oracle/config"
	"oracle/types"

	"github.com/IBM/sarama"
	"github.com/segmentio/kafka-go"
)

type BurnMessage struct {
	Address string `json:"address"`
	Stable  string `json:"stable"`
}

func StartBurnConsumer(db *sql.DB, writer *kafka.Writer) {
	fmt.Println("[Kafka: Burn] StartBurnConsumer 시작됨")

	brokers := config.KafkaBrokers
	topic := config.TopicBurnResult
	partition := int32(0)

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_1_0_0

	consumer, err := sarama.NewConsumer(brokers, saramaConfig)
	if err != nil {
		panic(fmt.Sprintf("[Kafka: Burn] Consumer 생성 실패: %v", err))
	}

	partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
	if err != nil {
		panic(fmt.Sprintf("[Kafka: Burn] 파티션 구독 실패: %v", err))
	}

	go func() {
		fmt.Println("[Kafka: Burn] Partition Consumer 수신 대기 중...")
		for msg := range partitionConsumer.Messages() {
			fmt.Printf("[Kafka: Burn] 수신 메시지: %s\n", string(msg.Value))

			// BurnMessage 파싱
			var burnMsg BurnMessage
			if err := json.Unmarshal(msg.Value, &burnMsg); err != nil {
				log.Printf("[Burn] JSON decode error: %v\n", err)
				continue
			}

			// stable에 "err"가 없을 때만 실행
			if burnMsg.Stable != "err" {
				HandleBurn(msg.Value, db, writer)
			} else {
				log.Printf("[Burn] stable 필드에 'err' 발견 → 처리 스킵")
			}
		}
	}()
}
func HandleBurn(msg []byte, db *sql.DB, writer *kafka.Writer) {
	// 1. BurnMessage 파싱
	var burnMsg BurnMessage
	if err := json.Unmarshal(msg, &burnMsg); err != nil {
		log.Printf("[Burn] BurnMessage JSON decode error: %v\n", err)
		return
	}

	// stable에서 숫자 추출 (예: "10stable" → 10)
	var count int
	_, err := fmt.Sscanf(burnMsg.Stable, "%dstable", &count)
	if err != nil || count <= 0 {
		log.Printf("[Burn] stable 값 파싱 실패: %s\n", burnMsg.Stable)
		return
	}

	// 2. Collaterals에서 count 개수 조회
	querySelect := fmt.Sprintf(`
		SELECT facility_id, facility_name, location, technology_type, capacity_mw,
		       registration_date, certified_id, issue_date,
		       generation_start_date, generation_end_date, measured_volume_mwh,
		       retired_date, retirement_purpose, status, timestamp
		FROM Collaterals
		ORDER BY timestamp ASC
		LIMIT %d
	`, count)

	rows, err := db.Query(querySelect)
	if err != nil {
		log.Printf("[Burn] Collaterals 조회 실패: %v\n", err)
		return
	}
	defer rows.Close()

	var recs []types.RECMeta
	for rows.Next() {
		var data types.RECMeta
		if err := rows.Scan(
			&data.FacilityID,
			&data.FacilityName,
			&data.Location,
			&data.TechnologyType,
			&data.CapacityMW,
			&data.RegistrationDate,
			&data.CertifiedId,
			&data.IssueDate,
			&data.GenerationStartDate,
			&data.GenerationEndDate,
			&data.MeasuredVolumeMWh,
			&data.RetiredDate,
			&data.RetirementPurpose,
			&data.Status,
			&data.Timestamp,
		); err != nil {
			log.Printf("[Burn] Row Scan 실패: %v\n", err)
			return
		}
		recs = append(recs, data)
	}

	if len(recs) == 0 {
		log.Printf("[Burn] Collaterals 데이터 없음")
		return
	}

	// 3. Kafka로 반환
	for _, rec := range recs {
		bytes, err := json.Marshal(rec)
		if err != nil {
			log.Printf("[Burn] JSON Marshal 실패: %v\n", err)
			continue
		}
		err = writer.WriteMessages(
			context.Background(),
			kafka.Message{Value: bytes},
		)
		if err != nil {
			log.Printf("[Burn] Kafka 메시지 전송 실패: %v\n", err)
			continue
		}
		log.Printf("[Burn] 라이트노드로 REC 반환 완료: %+v\n", rec)

		// 4. DELETE 처리
		queryDelete := `DELETE FROM Collaterals WHERE facility_id = $1 AND certified_id = $2`
		_, err = db.Exec(queryDelete, rec.FacilityID, rec.CertifiedId)
		if err != nil {
			log.Printf("[Burn] Collaterals 삭제 실패: %v\n", err)
		} else {
			log.Printf("[Burn] Collaterals에서 삭제 완료 (FacilityID=%s, CertifiedID=%s)", rec.FacilityID, rec.CertifiedId)
		}
	}
}
