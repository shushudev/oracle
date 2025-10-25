package connect

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"oracle/types"

	"github.com/segmentio/kafka-go"
	"golang.org/x/crypto/bcrypt"
)

// ConnectHandler : 사용자 등록 처리
func ConnectHandler(db *sql.DB, writer *kafka.Writer) http.HandlerFunc {
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
	}
}
