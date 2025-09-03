package consumer

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	conf "oracle/config"
)

// ===== 환경 변수 키 =====
const (
	envAuthKey      = "KMA_AUTH_KEY"
	envStation      = "KMA_STN"
	envTM           = "KMA_TM"
	envBackoffHours = "KMA_BACKOFF_HOURS"
	envOutputPath   = "KMA_OUT_PATH"
)

// ===== 출력 레코드 =====
type SolarRecord struct {
	Station string   `json:"관측지점"`
	Time    string   `json:"관측시간"`          // YYYY-MM-DD HH:MM
	SI      *float64 `json:"일사량,omitempty"` // MJ/m^2, 음수(결측 표기)는 null 처리
}

// ===== 외부 진입 함수 =====
func SaveSolarRadiationJSON(ctx context.Context) error {
	cfg, err := loadKMAConfig()
	if err != nil {
		return err
	}

	startTM := cfg.FixedTM
	if startTM == "" {
		startTM = nearestPastHourKST(time.Now().In(kst()))
	}

	var all []SolarRecord
	var usedTM string

	for i := 0; i <= cfg.BackoffHours; i++ {
		tmTry := minusHoursTM(startTM, i)

		var recs []SolarRecord
		if strings.EqualFold(cfg.Station, "ALL") {
			recs, err = collectAllStationsForTM(ctx, cfg.AuthKey, tmTry)
		} else {
			recs, err = collectSingleStationForTM(ctx, cfg.AuthKey, tmTry, cfg.Station)
		}

		if err == nil && len(recs) > 0 {
			all = recs
			usedTM = tmTry
			break
		}
		if cfg.FixedTM != "" { // 고정 tm 이면 한 번만 시도
			break
		}
	}

	if len(all) == 0 {
		return fmt.Errorf("no data found up to backoff=%dh (start tm=%s, stn=%s)", cfg.BackoffHours, startTM, cfg.Station)
	}

	// ✅ 평균 일사량 계산 (null 제외; 즉 음수 원데이터 제외)
	avg, n := averageSI(all)
	conf.KMAAverage = avg
	if n == 0 {
		fmt.Println("[WARN] no valid SI values; KMAAverage set to 0")
	} else {
		fmt.Printf("[OK] KMAAverage=%.6f (N=%d valid records)\n", avg, n)
	}

	// 기록용 JSON 저장
	out := cfg.OutputPath
	if out == "" {
		out = conf.KMAOutputPath
	}
	if err := writeJSON(out, all); err != nil {
		return err
	}

	fmt.Printf("[OK] %d records saved to %s (tm=%s)\n", len(all), out, usedTM)

	// 🔽🔽🔽 바로 여기 추가 🔽🔽🔽
	fmt.Printf("[REGION] joining %d records (tm=%s)\n", len(all), usedTM)
	if err := JoinAndAggregateByRegion(all /* []SolarRecord */, usedTM); err != nil {
		fmt.Printf("[REGION][ERROR] %v\n", err)
	} else {
		fmt.Printf("[REGION] wrote files:\n  - %s\n  - %s\n  - %s\n",
			conf.KMAStationRegionOut, conf.KMAJoinedOutPath, conf.KMARegionAggOutPath)
	}
	return nil

}

// ---- 평균 계산 헬퍼 ----
func averageSI(recs []SolarRecord) (avg float64, count int) {
	var sum float64
	for _, r := range recs {
		if r.SI != nil {
			sum += *r.SI
			count++
		}
	}
	if count == 0 {
		return 0, 0
	}
	return sum / float64(count), count
}

// ===== 구성 로딩 =====

type KMAConfig struct {
	AuthKey      string
	Station      string
	FixedTM      string // "" 면 자동 백오프
	BackoffHours int
	OutputPath   string
}

func loadKMAConfig() (KMAConfig, error) {
	// 1) env 우선
	auth := strings.TrimSpace(os.Getenv(envAuthKey))
	// 2) env 비었으면 config 폴백
	if auth == "" {
		auth = strings.TrimSpace(conf.KMAAuthKey)
	}
	if auth == "" {
		return KMAConfig{}, errors.New("KMA_AUTH_KEY is required (env or config.KMAAuthKey)")
	}

	stn := strings.TrimSpace(os.Getenv(envStation))
	if stn == "" {
		stn = conf.KMAStationDefault // 기본 ALL
	}

	tm := strings.TrimSpace(os.Getenv(envTM))

	bo := conf.KMABackoffHours
	if b := strings.TrimSpace(os.Getenv(envBackoffHours)); b != "" {
		if v, err := strconv.Atoi(b); err == nil && v >= 0 {
			bo = v
		}
	}

	out := strings.TrimSpace(os.Getenv(envOutputPath))
	if out == "" {
		out = conf.KMAOutputPath
	}

	return KMAConfig{
		AuthKey:      auth,
		Station:      stn,
		FixedTM:      tm,
		BackoffHours: bo,
		OutputPath:   out,
	}, nil
}

// ===== 공통 유틸 =====

func kst() *time.Location {
	loc, _ := time.LoadLocation("Asia/Seoul")
	return loc
}

func nearestPastHourKST(now time.Time) string {
	prev := now.Add(-1 * time.Hour).Truncate(time.Hour)
	return prev.Format("200601021504") // YYYYMMDDHHMM
}

func minusHoursTM(tm string, h int) string {
	t, _ := time.ParseInLocation("200601021504", tm, kst())
	t = t.Add(-time.Duration(h) * time.Hour)
	return t.Format("200601021504")
}

func httpClient() *http.Client {
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
			MaxIdleConns:        100,
			IdleConnTimeout:     30 * time.Second,
			TLSHandshakeTimeout: 10 * time.Second,
			DisableCompression:  false,
		},
	}
}

func fetchRawText(ctx context.Context, params map[string]string) (string, error) {
	u, _ := url.Parse(conf.KMAAPIURL)
	q := u.Query()
	for k, v := range params {
		if v != "" {
			q.Set(k, v)
		}
	}
	u.RawQuery = q.Encode()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	resp, err := httpClient().Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("http status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return string(body), nil
}

// ===== 파싱 =====

func parseTyp01TextToTable(text string) (headers []string, rows [][]string) {
	sc := bufio.NewScanner(strings.NewReader(text))
	var headerCandidates []string
	for sc.Scan() {
		line := strings.TrimRight(sc.Text(), "\r\n")
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			headerCandidates = append(headerCandidates, strings.TrimSpace(line[1:]))
			continue
		}
		if strings.TrimSpace(line) == "" {
			continue
		}
		fields := splitSpaces(line)
		if len(fields) > 0 {
			rows = append(rows, fields)
		}
	}
	for i := len(headerCandidates) - 1; i >= 0; i-- {
		toks := splitSpaces(headerCandidates[i])
		if len(toks) >= 5 && (strings.HasPrefix(strings.ToUpper(toks[0]), "YYMMDDHH") ||
			strings.Contains(strings.ToUpper(toks[0]), "YYMMDDHHMI")) {
			headers = toks
			break
		}
	}
	return headers, rows
}

func splitSpaces(s string) []string {
	return strings.FieldsFunc(s, func(r rune) bool { return r == ' ' || r == '\t' })
}

func indexOfHeader(headers []string, cands []string) int {
	if len(headers) == 0 {
		return -1
	}
	upper := make([]string, len(headers))
	for i, h := range headers {
		upper[i] = strings.ToUpper(strings.TrimSpace(h))
	}
	for _, c := range cands {
		cu := strings.ToUpper(c)
		for i, h := range upper {
			if h == cu {
				return i
			}
		}
	}
	return -1
}

func selectCoreColumns(headers []string, rows [][]string) []SolarRecord {
	if len(rows) == 0 {
		return nil
	}
	idxTM := indexOfHeader(headers, []string{"YYMMDDHHMI", "TM"})
	idxSTN := indexOfHeader(headers, []string{"STN", "ID"})
	idxSI := indexOfHeader(headers, []string{"SI"})
	if idxTM < 0 || idxSTN < 0 || idxSI < 0 {
		return nil
	}

	out := make([]SolarRecord, 0, len(rows))
	for _, r := range rows {
		if idxTM >= len(r) || idxSTN >= len(r) || idxSI >= len(r) {
			continue
		}
		rawTM := r[idxTM]   // YYYYMMDDHHMI
		rawSTN := r[idxSTN] // 지점 코드
		rawSI := r[idxSI]   // 일사량

		// 시간 포맷 변경
		humanTM := rawTM
		if t, err := time.ParseInLocation("200601021504", rawTM, kst()); err == nil {
			humanTM = t.Format("2006-01-02 15:04")
		}

		// SI 숫자화: 음수는 null (결측)
		var siPtr *float64
		if v, err := strconv.ParseFloat(rawSI, 64); err == nil {
			if v >= 0 {
				val := v
				siPtr = &val
			} else {
				siPtr = nil
			}
		}

		out = append(out, SolarRecord{
			Station: rawSTN,
			Time:    humanTM,
			SI:      siPtr,
		})
	}
	return out
}

// ===== 수집 로직 =====

func collectSingleStationForTM(ctx context.Context, authKey, tm, stn string) ([]SolarRecord, error) {
	params := map[string]string{
		"tm":      tm,
		"stn":     stn,
		"help":    "0",
		"authKey": authKey,
	}
	text, err := fetchRawText(ctx, params)
	if err != nil {
		return nil, err
	}
	headers, rows := parseTyp01TextToTable(text)
	recs := selectCoreColumns(headers, rows)
	return recs, nil
}

func collectAllStationsForTM(ctx context.Context, authKey, tm string) ([]SolarRecord, error) {
	{
		params := map[string]string{
			"tm":      tm,
			"help":    "0",
			"authKey": authKey,
		}
		if text, err := fetchRawText(ctx, params); err == nil {
			if h, rows := parseTyp01TextToTable(text); len(rows) > 0 {
				if recs := selectCoreColumns(h, rows); len(recs) > 0 {
					return recs, nil
				}
			}
		}
	}
	// (2) stn=0
	{
		params := map[string]string{
			"tm":      tm,
			"stn":     "0",
			"help":    "0",
			"authKey": authKey,
		}
		if text, err := fetchRawText(ctx, params); err == nil {
			if h, rows := parseTyp01TextToTable(text); len(rows) > 0 {
				if recs := selectCoreColumns(h, rows); len(recs) > 0 {
					return recs, nil
				}
			}
		}
	}

	return nil, errors.New("failed to fetch all stations with both 'no stn' and 'stn=0' calls")
}

// ===== 저장 =====
func writeJSON(path string, recs []SolarRecord) error {
	b, err := json.MarshalIndent(recs, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}
