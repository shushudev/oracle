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
			HandleResponseQuery(db, msg.Value)
		}
	}()
}

func HandleResponseQuery(db *sql.DB, msgValue []byte) {
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
		res, err := QueryTxByHashLCD(h)
		if err != nil {
			log.Printf("[Kafka: TxHashResponse] 해시 %s 조회 실패: %v", h, err)
			continue
		}
		log.Printf("[Kafka: TxHashResponse] 조회 성공 (Hash=%s): %s", h, res)
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

func QueryTxByHashLCD(hash string) (string, error) {
	url := fmt.Sprintf("http://192.168.0.19:1317/txs/%s", hash)
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}
