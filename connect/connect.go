package connect

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"oracle/types"

	"github.com/segmentio/kafka-go"
	"golang.org/x/crypto/bcrypt"
)

// ConnectHandler : 사용자 등록 처리
func ConnectHandler(db *sql.DB, writer, fullWriter *kafka.Writer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 1. JSON 요청 파싱
		var req types.ConnectRequest
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

		// 3-1 full node에 주소 정보 전달
		AccountCreateHandler(db, fullWriter, req)

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

func AccountCreateHandler(db *sql.DB, writer *kafka.Writer, res types.ConnectRequest) {
	var req types.AccountRequest

	req.NodeID = res.NodeID
	req.Address = res.Address

	// JSON 직렬화
	value, err := json.Marshal(req)
	if err != nil {
		fmt.Println("❌ AccountRequest 직렬화 실패:", err)
		return
	}

	// Kafka 메시지 생성
	msg := kafka.Message{
		Key:   []byte(req.NodeID), // 파티셔닝 기준 (선택 사항)
		Value: value,
	}

	// Kafka 토픽으로 전송
	err = writer.WriteMessages(context.Background(), msg)
	if err != nil {
		fmt.Println("[Kafka: Account] 주소 전송 실패:", err)
	} else {
		fmt.Println("[Kafka: Account] 주소 전송 성공:", req)
	}
}
