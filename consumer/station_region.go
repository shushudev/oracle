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

// ---------- 산출: 권역별 집계 ----------
type RegionAggregate struct {
	Time           string   `json:"관측시간"`
	Region         string   `json:"권역"`
	SampleCount    int      `json:"표본수"`
	SampleStations []string `json:"표본지점"` // ← 추가: 집계에 포함된 관측지점 ID 목록
	MeanSI         float64  `json:"평균일사량"`
}

// SaveSolarRadiationJSON에서 만든 rows([]SolarRecord)와 동일 패키지이므로 접근 가능
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
			Region:     deriveRegion(meta.Address, conf.RegionScheme), // 내부에서 bucketRegion 사용
			Time:       row.Time,
			Irradiance: row.SI,
		})
	}
	if err := writeJSONFile(conf.KMAJoinedOutPath, joined); err != nil {
		return fmt.Errorf("write joined: %w", err)
	}

	// 3) 권역별 집계(평균 + 표본 지점 ID)
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

// ---------- 권역 분류 ----------
func bucketRegion(addr string) string {
	a := strings.ReplaceAll(addr, " ", "")
	switch {
	case strings.Contains(a, "서울"):
		return "서울특별시"
	case strings.Contains(a, "경기도"), strings.Contains(a, "인천"):
		return "경기도"
	case strings.Contains(a, "제주"):
		return "제주특별시"
	case strings.Contains(a, "대구"), strings.Contains(a, "경상북도"), strings.Contains(a, "경북"):
		return "대구/경상북도"
	case strings.Contains(a, "대전"), strings.Contains(a, "충청남도"), strings.Contains(a, "충남"), strings.Contains(a, "세종"):
		return "대전/충청남도"
	case strings.Contains(a, "충청북도"), strings.Contains(a, "충북"):
		return "충청북도"
	case strings.Contains(a, "부산"), strings.Contains(a, "경상남도"), strings.Contains(a, "경남"), strings.Contains(a, "울산"):
		return "부산/경상남도"
	case strings.Contains(a, "광주"), strings.Contains(a, "전라남도"), strings.Contains(a, "전남"):
		return "광주/전라남도"
	case strings.Contains(a, "전라북도"), strings.Contains(a, "전북"):
		return "전라북도"
	case strings.Contains(a, "강원특별자치도"), strings.Contains(a, "강원"):
		return "강원도"
	default:
		return "기타/미분류"
	}
}

// 주소 → 권역 추론: 요구 사항에 따라 bucketRegion 규칙을 사용
func deriveRegion(address, _ string) string {
	if strings.TrimSpace(address) == "" {
		return "기타/미분류"
	}
	return bucketRegion(address)
}

func aggregateByRegion(joined []JoinedObservation) []RegionAggregate {
	type key struct{ t, r string }

	sum := map[key]float64{}
	cnt := map[key]int{}
	stations := map[key]map[string]struct{}{} // 권역-시간별 지점ID 집합(중복 제거)

	for _, x := range joined {
		if x.Irradiance == nil {
			continue
		}
		r := strings.TrimSpace(x.Region)
		if r == "" || r == "UNKNOWN" || r == "기타/미분류" {
			continue
		}
		k := key{t: x.Time, r: r}
		sum[k] += *x.Irradiance
		cnt[k]++
		if stations[k] == nil {
			stations[k] = make(map[string]struct{})
		}
		id := strings.TrimSpace(x.StationID)
		if id != "" {
			stations[k][id] = struct{}{}
		}
	}

	out := make([]RegionAggregate, 0, len(sum))
	for k, s := range sum {
		c := cnt[k]
		if c == 0 {
			continue
		}
		// 지점ID 목록 정렬
		ids := make([]string, 0, len(stations[k]))
		for id := range stations[k] {
			ids = append(ids, id)
		}
		sort.Strings(ids)

		out = append(out, RegionAggregate{
			Time:           k.t,
			Region:         k.r,
			SampleCount:    c,
			SampleStations: ids, // ← 추가 필드
			MeanSI:         s / float64(c),
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
