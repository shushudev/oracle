package connect

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/segmentio/kafka-go"
)

type ConnectRequest struct {
	NodeID     string `json:"node_id"`
	InverterID string `json:"inverter_id"`
	Password   string `json:"password"`
	PublicKey  string `json:"public_key"`
	Address    string `json:"address"`
}

func ConnectHandler(db *sql.DB, writer *kafka.Writer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// JSON 요청 바디 파싱
		var req ConnectRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		// INSERT 쿼리 실행
		query := `INSERT INTO userData (node_id, inverter_id, password, public_key, created_at, address)
				  VALUES ($1, $2, $3, $4, $5, $6)`
		_, err = db.Exec(query, req.NodeID, req.InverterID, req.Password, req.PublicKey, time.Now(), req.Address)
		if err != nil {
			log.Printf("[ConnectHandler] DB insert error: %v", err)
			http.Error(w, "Failed to insert user data", http.StatusInternalServerError)
			return
		}

		// Kafka 메시지 발행 (예: 가입 이벤트)
		msgBytes, err := json.Marshal(req)
		if err != nil {
			log.Printf("[ConnectHandler] Kafka marshal error: %v", err)
		} else {
			err = writer.WriteMessages(r.Context(), kafka.Message{
				Key:   []byte(req.NodeID),
				Value: msgBytes,
			})
			if err != nil {
				log.Printf("[ConnectHandler] Kafka write error: %v", err)
			}
		}

		// 성공 응답
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"success","message":"User registered"}`))
	}
}
