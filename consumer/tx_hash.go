package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"oracle/config"
	"oracle/types"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/segmentio/kafka-go"
)

type TxHashResult struct {
	Address string `json:"address"`
	Hash    string `json:"hash"`
}

func StartTxHashConsumer(db *sql.DB) { // 풀노드로부터 받은 해시값을 db에 저장하는 함수

	brokers := config.KafkaBrokers
	topic := config.TopicTxHash
	partition := int32(0)

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_1_0_0

	consumer, err := sarama.NewConsumer(brokers, saramaConfig)
	if err != nil {
		panic(fmt.Sprintf("[Kafka: TxHash] Consumer 생성 실패: %v", err))
	}

	partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
	if err != nil {
		panic(fmt.Sprintf("[Kafka: TxHash] 파티션 구독 실패: %v", err))
	}

	go func() {
		fmt.Println("[Kafka: TxHash] Partition Consumer 수신 대기 중...")
		for msg := range partitionConsumer.Messages() {
			fmt.Printf("[Kafka: TxHash] 수신 메시지: %s\n", string(msg.Value))

			// 메시지를 처리하는 기존 로직 호출
			HandleQuery(db, msg.Value)
		}
	}()
}

func HandleQuery(db *sql.DB, msgValue []byte) {
	var result TxHashResult
	if err := json.Unmarshal(msgValue, &result); err != nil {
		log.Printf("[Kafka: TxHash] JSON Unmarshal 실패: %v", err)
		return
	}

	// hash에 UNIQUE 제약이 있다는 전제
	query := `
		INSERT INTO solar_archive (address, hash)
		VALUES ($1, $2)
		ON CONFLICT (hash) DO NOTHING
	`
	res, err := db.Exec(query, result.Address, result.Hash)
	if err != nil {
		log.Printf("[Kafka: TxHash] DB Insert 실패 (Address: %s, Hash: %s): %v", result.Address, result.Hash, err)
		return
	}

	rows, _ := res.RowsAffected()
	if rows == 0 {
		log.Printf("[Kafka: TxHash] 중복된 해시값 (hash: %s) → 삽입 생략", result.Hash)
		return
	}
	log.Printf("[Kafka: TxHash] DB 기록 성공: Address=%s, Hash=%s", result.Address, result.Hash)
}

func StartRequestTxHashConsumer(db *sql.DB, writer *kafka.Writer) { // 라이트노드로 부터 받은 주소에 해당하는 해시 조회

	brokers := config.KafkaBrokers
	topic := config.TopicRequestTxHash
	partition := int32(0)

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_1_0_0

	consumer, err := sarama.NewConsumer(brokers, saramaConfig)
	if err != nil {
		panic(fmt.Sprintf("[Kafka: Light TxHash] Consumer 생성 실패: %v", err))
	}

	partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
	if err != nil {
		panic(fmt.Sprintf("[Kafka: Light TxHash] 파티션 구독 실패: %v", err))
	}

	go func() {
		fmt.Println("[Kafka: Light TxHash] Partition Consumer 수신 대기 중...")
		for msg := range partitionConsumer.Messages() {
			fmt.Printf("[Kafka: Light TxHash] 수신 메시지: %s\n", string(msg.Value))

			// 메시지를 처리하는 기존 로직 호출
			HandleResponseQuery(db, msg.Value, writer)
		}
	}()
}

type tmTxRPC struct {
	Result struct {
		Hash     string `json:"hash"`
		Height   string `json:"height"`
		TxResult struct {
			Code int    `json:"code"`
			Log  string `json:"log"` // JSON 문자열
		} `json:"tx_result"`
	} `json:"result"`
}

type logRoot []struct {
	Events []struct {
		Type       string `json:"type"`
		Attributes []struct {
			Key   string  `json:"key"`
			Value *string `json:"value"`
		} `json:"attributes"`
	} `json:"events"`
}

// ── 2) 모바일용 최소 필드 ───────────────────────────────────────────────

type TxBrief struct {
	Address     string  `json:"address"`
	Height      string  `json:"height"`
	TxHash      string  `json:"txhash"`
	DeviceID    string  `json:"device_id,omitempty"`
	Timestamp   string  `json:"timestamp,omitempty"`
	TotalEnergy float64 `json:"total_energy,omitempty"`
	Latitude    float64 `json:"latitude,omitempty"`
	Longitude   float64 `json:"longitude,omitempty"`
}

func QueryTxBriefViaRPC(rpcHost string, rpcPort int, hash string) (*TxBrief, error) {
	// Tendermint RPC: 0x 접두사 필수
	url := fmt.Sprintf("http://%s:%d/tx?hash=0x%s", rpcHost, rpcPort, strings.ToUpper(hash))

	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("http get: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}

	// 1차 언마샬 (RPC)
	var rpc tmTxRPC
	if err := json.Unmarshal(body, &rpc); err != nil {
		return nil, fmt.Errorf("rpc unmarshal: %w", err)
	}
	if rpc.Result.TxResult.Code != 0 {
		return nil, fmt.Errorf("tx failed: code=%d", rpc.Result.TxResult.Code)
	}

	// 2차 언마샬 (log 문자열)
	var logs logRoot
	if err := json.Unmarshal([]byte(rpc.Result.TxResult.Log), &logs); err != nil {
		return nil, fmt.Errorf("log unmarshal: %w", err)
	}

	out := &TxBrief{
		Height: rpc.Result.Height,
		TxHash: rpc.Result.Hash,
	}

	// light_tx_solar 이벤트에서 필요한 속성만 추출
	for _, entry := range logs {
		for _, ev := range entry.Events {
			if ev.Type != "light_tx_solar" {
				continue
			}
			// attr map
			var (
				deviceID, ts          string
				latStr, lonStr, teStr string
			)
			for _, a := range ev.Attributes {
				key := a.Key
				val := ""
				if a.Value != nil {
					val = *a.Value
				}
				switch key {
				case "device_id":
					deviceID = val
				case "timestamp":
					ts = val
				case "total_energy":
					teStr = val
				case "latitude":
					latStr = val
				case "longitude":
					lonStr = val
				}
			}

			out.DeviceID = deviceID
			out.Timestamp = ts

			out.Address = ""

			if teStr != "" {
				if f, err := strconv.ParseFloat(teStr, 64); err == nil {
					out.TotalEnergy = f
				}
			}
			if latStr != "" {
				if f, err := strconv.ParseFloat(latStr, 64); err == nil {
					out.Latitude = f
				}
			}
			if lonStr != "" {
				if f, err := strconv.ParseFloat(lonStr, 64); err == nil {
					out.Longitude = f
				}
			}
			// 첫 solar 이벤트만 사용
			return out, nil
		}
	}
	// solar 이벤트가 없으면 최소 필드만 반환
	return out, nil
}

func HandleResponseQuery(db *sql.DB, msgValue []byte, writer *kafka.Writer) {

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Unmarshal (라이트노드에서 넘어온 요청)
	var req types.TxHashRequest
	if err := json.Unmarshal(msgValue, &req); err != nil {
		log.Printf("[Kafka: TxHashResponse] JSON Unmarshal 실패: %v", err)
		return
	}

	// 2. DB에서 해당 주소의 해시 목록 가져오기
	hashes, err := GetTxHashesByAddress(db, req.Address)
	if err != nil {
		log.Printf("[Kafka: TxHashResponse] DB 조회 실패: %v", err)
		return
	}
	if len(hashes) == 0 {
		log.Printf("[Kafka: TxHashResponse] 해당 주소 %s 에 대한 해시 없음", req.Address)
		return
	}

	// 3. 각 해시별로 LCD 호출
	for _, h := range hashes {
		brief, err := QueryTxBriefViaRPC("192.168.0.19", 26657, h)
		if err != nil {
			log.Printf("[Kafka: TxHashResponse] 해시 %s 조회 실패: %v", h, err)
			continue
		}
		brief.Address = req.Address

		b, _ := json.Marshal(brief)

		log.Printf("[Kafka: TxHashResponse] 조회 성공 (Hash=%s): %s", h, string(b))

		if writer != nil {
			if err := writer.WriteMessages(ctx, kafka.Message{
				Key:   []byte(req.Address), // 같은 주소는 같은 파티션
				Value: b,
			}); err != nil {
				log.Printf("[Kafka: TxHashResponse] Kafka 전송 실패 (hash=%s): %v", h, err)
			}
		}
	}
}

func GetTxHashesByAddress(db *sql.DB, address string) ([]string, error) {
	query := `SELECT hash FROM solar_archive WHERE address = $1`
	rows, err := db.Query(query, address)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var hashes []string
	for rows.Next() {
		var hash string
		if err := rows.Scan(&hash); err != nil {
			return nil, err
		}
		hashes = append(hashes, hash)
	}
	return hashes, nil
}
