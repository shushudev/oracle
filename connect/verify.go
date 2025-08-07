package connect

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"

	"golang.org/x/crypto/bcrypt"
)

// 로그인 요청 구조체
type VerifyRequest struct {
	NodeID   string `json:"node_id"`
	DeviceID string `json:"device_id"`
	Password string `json:"password"`
}

// VerifyHandler : 로그인 검증
func VerifyHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		enableCORS(w)

		// ✅ OPTIONS 요청 처리 (CORS Preflight)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		// ✅ POST 요청만 허용
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{
				"status":  "fail",
				"message": "Method not allowed",
			})
			return
		}

		// ✅ 요청 Body 파싱
		var req VerifyRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			log.Printf("[VerifyHandler] JSON decode error: %v", err)
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"status":  "fail",
				"message": "Invalid request",
			})
			return
		}

		// ✅ DB에서 비밀번호 조회
		var hashedPassword, address string
		query := `SELECT password, address FROM userData WHERE node_id=$1 AND device_id=$2`
		err = db.QueryRow(query, req.NodeID, req.DeviceID).Scan(&hashedPassword, &address)
		if err != nil {
			log.Printf("[VerifyHandler] User not found: node_id=%s, device_id=%s", req.NodeID, req.DeviceID)
			writeJSON(w, http.StatusUnauthorized, map[string]string{
				"status":  "fail",
				"message": "Invalid credentials",
			})
			return
		}

		// ✅ bcrypt 비교
		if bcrypt.CompareHashAndPassword([]byte(hashedPassword), []byte(req.Password)) != nil {
			log.Printf("[VerifyHandler] Password mismatch: node_id=%s", req.NodeID)
			writeJSON(w, http.StatusUnauthorized, map[string]string{
				"status":  "fail",
				"message": "Invalid credentials",
			})
			return
		}

		// ✅ 성공 응답
		writeJSON(w, http.StatusOK, map[string]string{
			"status":  "success",
			"message": "Login successful",
			"address": address,
		})
	}
}

// ✅ 공통 JSON 응답 함수
func writeJSON(w http.ResponseWriter, status int, data map[string]string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

// ✅ CORS 허용
func enableCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
}
