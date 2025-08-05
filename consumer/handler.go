package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"oracle/model"
	"strconv"

	"github.com/segmentio/kafka-go"
)

func fetchExpectedIrradiance(lat, lon float64, timestamp string) float64 {
	return 900.0 // NASA API 연동 가능
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
	SenderID string `json:"sender_id"`
}

type MappingResponse struct {
	DeviceID string `json:"device_id"`
	Address  string `json:"address"`
	SenderID string `json:"sender_id"`
}

// DB에서 address 조회
func LookupAddressFromDB(db *sql.DB, DeviceID string) string {
	var address string
	err := db.QueryRow("SELECT address FROM userData WHERE device_id = $1", DeviceID).Scan(&address)
	if err != nil {
		log.Printf("[Mapping] DB query error for device_id=%s: %v", DeviceID, err)
		return ""
	}
	return address
}

func HandleMappingRequest(msg []byte, db *sql.DB, writer *kafka.Writer) {
	var req MappingRequest
	if err := json.Unmarshal(msg, &req); err != nil {
		log.Printf("[Mapping] JSON decode error: %v\n", err)
		return
	}

	address := LookupAddressFromDB(db, req.DeviceID)
	if address == "" {
		log.Printf("[Mapping] No address found for device_id=%s", req.DeviceID)
		return
	}

	resp := MappingResponse{
		DeviceID: req.DeviceID,
		Address:  address,
		SenderID: req.SenderID,
	}

	respBytes, _ := json.Marshal(resp)

	err := writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(req.DeviceID),
		Value: respBytes,
	})
	if err != nil {
		log.Printf("❌ Kafka publish error: %v\n", err)
		return
	}

	log.Printf("[Mapping] device_id=%s → address=%s", req.DeviceID, address)
}

// 인구 밀도 조사 api
const (
	BaseURL        = "https://sgisapi.kostat.go.kr/OpenAPI3/"
	ConsumerKey    = "355de19f342d4390a161"
	ConsumerSecret = "9990cf6a197c429d8db1"
)

type TokenResponse struct {
	ErrCd  int    `json:"errCd"`
	ErrMsg string `json:"errMsg"`
	Result struct {
		AccessToken string `json:"accessToken"`
	} `json:"result"`
}

// 0️⃣ AccessToken 발급
func GetAccessToken() (string, error) {
	reqUrl := fmt.Sprintf("%sauth/authentication.json", BaseURL)
	params := url.Values{
		"consumer_key":    {ConsumerKey},
		"consumer_secret": {ConsumerSecret},
	}
	resp, err := http.Get(reqUrl + "?" + params.Encode())
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	var tokenResp struct {
		ErrCd  int    `json:"errCd"`
		ErrMsg string `json:"errMsg"`
		Result struct {
			AccessToken string `json:"accessToken"`
		} `json:"result"`
	}

	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return "", err
	}

	if tokenResp.ErrCd != 0 {
		return "", fmt.Errorf("accessToken 요청 실패: errCd=%d", tokenResp.ErrCd)
	}

	return tokenResp.Result.AccessToken, nil
}

// 1️⃣ UTMK 좌표 변환
func ConvertToUTMK(lat, lon float64, accessToken string) (x, y float64, err error) {
	reqUrl := fmt.Sprintf("%stransformation/transcoord.json", BaseURL)
	params := url.Values{
		"src":         {"4326"},
		"dst":         {"5179"},
		"posX":        {fmt.Sprintf("%.6f", lon)},
		"posY":        {fmt.Sprintf("%.6f", lat)},
		"accessToken": {accessToken},
	}
	resp, err := http.Get(reqUrl + "?" + params.Encode())
	if err != nil {
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var r struct {
		Result struct {
			PosX float64 `json:"posX"`
			PosY float64 `json:"posY"`
		} `json:"result"`
	}
	if err = json.Unmarshal(body, &r); err != nil {
		return
	}
	x = r.Result.PosX
	y = r.Result.PosY
	return
}

// 2️⃣ 격자 코드 조회
func FindAdmCode(x, y float64, accessToken string) (admCode string, err error) {
	reqUrl := fmt.Sprintf("%spersonal/findcodeinsmallarea.json", BaseURL)
	params := url.Values{
		"x_coor":      {fmt.Sprintf("%.2f", x)},
		"y_coor":      {fmt.Sprintf("%.2f", y)},
		"accessToken": {accessToken},
	}
	resp, err := http.Get(reqUrl + "?" + params.Encode())
	if err != nil {
		return
	}
	defer resp.Body.Close()
	var r struct {
		Result struct {
			SidoCd string `json:"sido_cd"`
			SggCd  string `json:"sgg_cd"`
		} `json:"result"`
	}

	body, _ := io.ReadAll(resp.Body)

	err = json.Unmarshal(body, &r)
	if err == nil {
		admCode = r.Result.SidoCd + r.Result.SggCd
	}
	return
}

// 3️⃣ 인구 조회 (population 통계)
func GetPopulation(admCode string, year string, accessToken string) (int, error) {
	reqUrl := fmt.Sprintf("%sstats/searchpopulation.json", BaseURL)
	params := url.Values{
		"adm_cd":      {admCode},
		"year":        {year},
		"accessToken": {accessToken},
	}

	resp, err := http.Get(reqUrl + "?" + params.Encode())
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	var r struct {
		Result []struct {
			Population string `json:"population"`
		} `json:"result"`
	}

	body, _ := io.ReadAll(resp.Body)

	if err := json.Unmarshal(body, &r); err != nil {
		return 0, err
	}
	if len(r.Result) > 0 {
		return strconv.Atoi(r.Result[0].Population)
	}
	return 0, fmt.Errorf("population not found")
}

// 🧩 전체 실행 함수
func GetPopulationByLatLon(lat, lon float64, year string) (int, error) {
	// Step 0: 토큰 발급
	accessToken, err := GetAccessToken()
	if err != nil {
		return 0, fmt.Errorf("access token 발급 실패: %v", err)
	}

	// Step 1: 좌표 변환
	x, y, err := ConvertToUTMK(lat, lon, accessToken)
	if err != nil {
		return 0, fmt.Errorf("좌표 변환 실패: %v", err)
	}

	// Step 2: 격자 코드 조회
	admCode, err := FindAdmCode(x, y, accessToken)
	if err != nil {
		return 0, fmt.Errorf("격자 코드 조회 실패: %v", err)
	}

	// Step 3: 인구 수 조회
	population, err := GetPopulation(admCode, year, accessToken)
	if err != nil {
		return 0, fmt.Errorf("인구 수 조회 실패: %v", err)
	}

	return population, nil
}
