package consumer

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	conf "oracle/config"
)

// ---------- 입력 스키마: solar_stations.json ----------
type StationMeta struct {
	StationID string `json:"지점"`
	Name      string `json:"지점명"`
	Address   string `json:"지점주소"`
}

// ---------- 산출: 지점→권역 매핑 테이블 ----------
type StationRegion struct {
	StationID string `json:"지점"`
	Name      string `json:"지점명"`
	Address   string `json:"지점주소"`
	Region    string `json:"권역"`
}

// ---------- 산출: 관측치 + 메타 조인 ----------
type JoinedObservation struct {
	StationID  string   `json:"관측지점"`
	Name       string   `json:"지점명"`
	Address    string   `json:"지점주소"`
	Region     string   `json:"권역"`
	Time       string   `json:"관측시간"`
	Irradiance *float64 `json:"일사량"` // MJ/m^2; 결측은 null
}

// ---------- 산출: 권역별 집계(우선 평균) ----------
type RegionAggregate struct {
	Time        string  `json:"관측시간"`
	Region      string  `json:"권역"`
	SampleCount int     `json:"표본수"`
	MeanSI      float64 `json:"평균일사량"`
}

// SaveSolarRadiationJSON에서 만든 rows([]SolarRow)와 동일 패키지이므로 접근 가능
// tm은 SaveSolarRadiationJSON 내에서 쓰던 관측 기준 시각 문자열

func JoinAndAggregateByRegion(rows []SolarRecord, tm string) error {
	// 1) 관측소 메타 로드
	stations, err := loadStations(conf.KMAStationsPath)
	if err != nil {
		return err
	}

	// station → region 매핑 산출
	sregs := make([]StationRegion, 0, len(stations))
	stationToRegion := make(map[string]string, len(stations))
	for sid, meta := range stations {
		r := deriveRegion(meta.Address, conf.RegionScheme)
		stationToRegion[sid] = r
		sregs = append(sregs, StationRegion{
			StationID: sid,
			Name:      meta.Name,
			Address:   meta.Address,
			Region:    r,
		})
	}
	if err := writeJSONFile(conf.KMAStationRegionOut, sregs); err != nil {
		return fmt.Errorf("write station-region: %w", err)
	}

	// 2) 관측치 + 메타 조인
	joined := make([]JoinedObservation, 0, len(rows))
	for _, row := range rows {
		meta, ok := stations[row.Station]
		if !ok {
			// 관측치에 있으나 메타에 없는 지점 → 스킵 (원하면 로그만 찍고 포함 가능)
			continue
		}
		joined = append(joined, JoinedObservation{
			StationID:  row.Station,
			Name:       meta.Name,
			Address:    meta.Address,
			Region:     deriveRegion(meta.Address, conf.RegionScheme),
			Time:       row.Time,
			Irradiance: row.SI,
		})
	}
	if err := writeJSONFile(conf.KMAJoinedOutPath, joined); err != nil {
		return fmt.Errorf("write joined: %w", err)
	}

	// 3) 권역별 집계(평균)
	agg := aggregateByRegion(joined)
	sort.SliceStable(agg, func(i, j int) bool {
		if agg[i].Time == agg[j].Time {
			return agg[i].Region < agg[j].Region
		}
		return agg[i].Time < agg[j].Time
	})
	if err := writeJSONFile(conf.KMARegionAggOutPath, agg); err != nil {
		return fmt.Errorf("write region-agg: %w", err)
	}

	return nil
}

// ---------------- 내부 유틸 ----------------

func loadStations(path string) (map[string]StationMeta, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read stations: %w", err)
	}
	var list []StationMeta
	if err := json.Unmarshal(b, &list); err != nil {
		return nil, fmt.Errorf("parse stations: %w", err)
	}
	m := make(map[string]StationMeta, len(list))
	for _, s := range list {
		sid := strings.TrimSpace(s.StationID)
		if sid != "" {
			m[sid] = s
		}
	}
	if len(m) == 0 {
		return nil, errors.New("no stations loaded")
	}
	return m, nil
}

// 주소 첫 토큰(시/도)을 기준으로 권역 추론
func deriveRegion(address, scheme string) string {
	first := firstToken(strings.TrimSpace(address))
	if first == "" {
		return "UNKNOWN"
	}
	if scheme == "Sido17" {
		return normalizeSido(first)
	}
	// Macro6 (수도/강원/충청/호남/영남/제주)
	sido := normalizeSido(first)
	switch {
	case matchAny(sido, "서울특별시", "경기도", "인천광역시"):
		return "수도권"
	case matchAny(sido, "강원특별자치도", "강원도"):
		return "강원권"
	case matchAny(sido, "충청북도", "충청남도", "세종특별자치시", "대전광역시"):
		return "충청권"
	case matchAny(sido, "전라북도", "전라남도", "광주광역시"):
		return "호남권"
	case matchAny(sido, "경상북도", "경상남도", "대구광역시", "부산광역시", "울산광역시"):
		return "영남권"
	case matchAny(sido, "제주특별자치도", "제주도"):
		return "제주권"
	default:
		return "UNKNOWN"
	}
}

func firstToken(addr string) string {
	seps := []string{" ", "(", ")", ",", "·"}
	for _, sp := range seps {
		if i := strings.Index(addr, sp); i > 0 {
			return addr[:i]
		}
	}
	return addr
}

func normalizeSido(s string) string {
	s = strings.TrimSpace(s)
	repl := map[string]string{
		"강원도": "강원특별자치도",
		"제주도": "제주특별자치도",
	}
	if v, ok := repl[s]; ok {
		return v
	}
	return s
}

func matchAny(s string, list ...string) bool {
	for _, v := range list {
		if s == v {
			return true
		}
	}
	return false
}

func aggregateByRegion(joined []JoinedObservation) []RegionAggregate {
	type key struct{ t, r string }
	sum := map[key]float64{}
	cnt := map[key]int{}

	for _, x := range joined {
		if x.Irradiance == nil {
			continue
		}
		r := strings.TrimSpace(x.Region)
		if r == "" || r == "UNKNOWN" {
			continue // 필요 시 포함 가능
		}
		k := key{t: x.Time, r: r}
		sum[k] += *x.Irradiance
		cnt[k]++
	}

	out := make([]RegionAggregate, 0, len(sum))
	for k, s := range sum {
		c := cnt[k]
		if c == 0 {
			continue
		}
		out = append(out, RegionAggregate{
			Time:        k.t,
			Region:      k.r,
			SampleCount: c,
			MeanSI:      s / float64(c),
		})
	}
	return out
}

func writeJSONFile(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0644)
}
