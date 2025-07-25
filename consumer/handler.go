package consumer

import (
	"encoding/json"
	"log"
	"math"
	"oracle/model"
	"context"
	"database/sql"
	"github.com/segmentio/kafka-go"
)

func fetchExpectedIrradiance(lat, lon float64, timestamp string) float64 {
	// 외부 API 연동 가능 (NASA 등)
	return 900.0
}

func HandleMessage(msg []byte) {
	var data model.SolarData
	if err := json.Unmarshal(msg, &data); err != nil {
		log.Printf("❌ JSON decode error: %v\n", err)
		return
	}

	expected := fetchExpectedIrradiance(data.Latitude, data.Longitude, data.Timestamp)
	diff := math.Abs(expected - data.Irradiance)

	log.Printf("🌞 Device=%s, Reported=%.1f, Expected=%.1f, Δ=%.1f",
		data.DeviceID, data.Irradiance, expected, diff)

	if diff <= 150 {
		log.Println("✅ Valid irradiance")
	} else {
		log.Println("⚠️ Suspicious data")
	}
}

type MappingRequest struct {
	DeviceID string `json:"device_id"`
}

type MappingResponse struct {
	DeviceID string `json:"device_id"`
	Address  string `json:"address"`
}

func HandleMappingRequest(msg []byte, db *sql.DB, writer *kafka.Writer) {
	var req MappingRequest
	if err := json.Unmarshal(msg, &req); err != nil {
		log.Printf("[Mapping] JSON decode error: %v\n", err)
		return
	}

	// DB 쿼리
	var address string
	err := db.QueryRow("SELECT address FROM device_mapping WHERE device_id = $1", req.DeviceID).Scan(&address)
	if err != nil {
		log.Printf("[Mapping] DB query error for device_id=%s: %v\n", req.DeviceID, err)
		return
	}

	// 응답 메시지 구성
	resp := MappingResponse{
		DeviceID: req.DeviceID,
		Address:  address,
	}
	respBytes, _ := json.Marshal(resp)

	// Kafka로 응답 발행
	err = writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(req.DeviceID),
		Value: respBytes,
	})
	if err != nil {
		log.Printf("❌ Kafka publish error: %v\n", err)
		return
	}

	log.Printf("[Mapping] DeviceID %s mapped to address %s", req.DeviceID, address)
}