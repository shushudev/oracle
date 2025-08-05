package connect

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/segmentio/kafka-go"
	"golang.org/x/crypto/bcrypt"
)

// 요청 구조체
type ConnectRequest struct {
	NodeID    string `json:"node_id"`
	DeviceID  string `json:"device_id"`
	Password  string `json:"password"`
	PublicKey string `json:"public_key"`
	Address   string `json:"address"`
}

// ConnectHandler : 사용자 등록 처리
func ConnectHandler(db *sql.DB, writer *kafka.Writer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 1. JSON 요청 파싱
		var req ConnectRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, `{"status":"fail","message":"Invalid request body"}`, http.StatusBadRequest)
			return
		}

		// 2. 비밀번호 해싱
		hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
		if err != nil {
			log.Printf("[ConnectHandler] Password hash error: %v", err)
			http.Error(w, `{"status":"fail","message":"Password processing error"}`, http.StatusInternalServerError)
			return
		}

		// 3. DB에 사용자 데이터 저장
		query := `INSERT INTO userData (node_id, device_id, password, public_key, created_at, address)
				  VALUES ($1, $2, $3, $4, $5, $6)`
		_, err = db.Exec(query, req.NodeID, req.DeviceID, string(hashedPassword), req.PublicKey, time.Now(), req.Address)
		if err != nil {
			log.Printf("[ConnectHandler] DB insert error: %v", err)
			http.Error(w, `{"status":"fail","message":"Failed to insert user data"}`, http.StatusInternalServerError)
			return
		}

		// 4. Kafka 메시지 발행 (가입 이벤트)
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

		// 5. 성공 응답
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"success","message":"User registered successfully"}`))
	}
}
