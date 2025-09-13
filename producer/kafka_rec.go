package producer

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"oracle/config"

	"github.com/IBM/sarama"
)

type recMessage struct {
	Price string `json:"price"`
}

type ApiResponse struct {
	Response struct {
		Header struct {
			ResultCode string `json:"resultCode"`
			ResultMsg  string `json:"resultMsg"`
		} `json:"header"`
		Body struct {
			Items struct {
				Item struct {
					RecCount int    `json:"reccount"`
					RecPrice string `json:"recprice"`
					Ymd      int    `json:"ymd"`
				} `json:"item"`
			} `json:"items"`
		} `json:"body"`
	} `json:"response"`
}

// 외부 API에서 REC 가격 조회
func fetchRECPrice() (string, error) {
	apiURL := "http://www.iwest.co.kr:8082/openapi-data/service/TradeList/Trade" +
		"?serviceKey=f40ef9decfc2b0e8ece558730bb5787def78b25bd23b5d5a4eca6c039961e9ba" +
		"&pageNo=1&numOfRows=1&resultType=json"

	resp, err := http.Get(apiURL)
	if err != nil {
		return "", fmt.Errorf("REC 가격 API 호출 실패: %w", err)
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)

	var apiResp ApiResponse
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return "", fmt.Errorf("REC 가격 JSON 파싱 실패: %w", err)
	}

	if apiResp.Response.Body.Items.Item.RecPrice == "" {
		return "", fmt.Errorf("API 응답에 가격 데이터 없음")
	}

	// 가장 최근 거래가격 반환
	return apiResp.Response.Body.Items.Item.RecPrice, nil
}

// Kafka Producer 실행
func StartOracleProducer(producer sarama.SyncProducer) {
	topic := config.TopicRECPrice // REC 가격을 전송할 Kafka 토픽

	ticker := time.NewTicker(1 * time.Minute) // 1분마다 실행
	defer ticker.Stop()

	for range ticker.C {
		// price, err := fetchRECPrice()
		// if err != nil {
		// 	log.Printf("[Oracle] 가격 가져오기 실패: %v", err)
		// 	continue
		// }

		msg := recMessage{Price: string("71,400")}
		value, _ := json.Marshal(msg)

		kafkaMsg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(value),
		}

		partition, offset, err := producer.SendMessage(kafkaMsg)
		if err != nil {
			log.Printf("[Oracle] Kafka 전송 실패: %v", err)
		} else {
			// log.Printf("[Oracle] REC 가격 전송 완료: %s (partition=%d, offset=%d)",
			// 	price, partition, offset)
			log.Printf("[Oracle] REC 가격 전송 완료: 71,400 (partition=%d, offset=%d)",
				partition, offset)
		}
	}

}
