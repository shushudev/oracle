package consumer

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"

	conf "oracle/config"
)

// station_region.go에서 저장하는 조인 JSON과 호환되는 최소 스키마
type joinedRow struct {
	StationID  string   `json:"관측지점"`
	Region     string   `json:"권역"`
	Time       string   `json:"관측시간"`
	Irradiance *float64 `json:"일사량"`
}

// R0, q*, q10, q90, 사용된 권역 수를 리턴
func ComputeR0FromJoined(joinedPath string, enableInverse bool, B, qlo, qhi float64) (r0, qstar, q10, q90 float64, regionsUsed int, err error) {
	rows, err := readJoined(joinedPath)
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}
	if len(rows) == 0 {
		return 0, 0, 0, 0, 0, fmt.Errorf("no joined rows")
	}

	// 1) 전체 유효 x 수집
	var allX []float64
	for _, r := range rows {
		if r.Irradiance == nil {
			continue
		}
		// 음수는 Step1에서 이미 null로 걸렀지만, 혹시 모를 값 방어
		if *r.Irradiance <= -9.0 {
			continue
		}
		allX = append(allX, *r.Irradiance)
	}
	if len(allX) == 0 {
		return 0, 0, 0, 0, 0, fmt.Errorf("no valid irradiance values")
	}
	sort.Float64s(allX)

	// 2) 분위수 계산
	q10 = quantileSorted(allX, qlo)
	q90 = quantileSorted(allX, qhi)
	if q90 == q10 {
		minv, maxv := allX[0], allX[len(allX)-1]
		if maxv == minv {
			q10 = minv
			q90 = minv + 1e-9
		} else {
			q10, q90 = minv, maxv
		}
	}

	// 3) z-정규화 후 권역별 중앙값 m_r
	regionZ := map[string][]float64{}
	for _, r := range rows {
		if r.Irradiance == nil || strings.TrimSpace(r.StationID) == "" {
			continue
		}
		z := toZ(*r.Irradiance, q10, q90) // [0,1]
		regionZ[strings.TrimSpace(r.Region)] = append(regionZ[strings.TrimSpace(r.Region)], z)
	}

	type pair struct {
		Region string
		Mr     float64
		N      int
	}
	var plist []pair
	for region, zs := range regionZ {
		if len(zs) == 0 {
			continue
		}
		m := median(zs)
		if conf.RequestedRegions[region] {
			plist = append(plist, pair{Region: region, Mr: m, N: len(zs)})
		}
	}
	if len(plist) == 0 {
		return 0, 0, q10, q90, 0, fmt.Errorf("no requested regions present")
	}

	// 4) q* = mean_r(m_r)
	var sum float64
	for _, p := range plist {
		sum += p.Mr
	}
	qstar = sum / float64(len(plist))
	regionsUsed = len(plist)

	// 5) R0
	var raw float64
	if enableInverse {
		p := qlo + qstar*(qhi-qlo)
		if p < 0 {
			p = 0
		} else if p > 1 {
			p = 1
		}
		raw = invECDF(allX, p) // MJ/m^2
	} else {
		raw = qstar // [0,1]
	}
	r0 = B * raw
	return r0, qstar, q10, q90, regionsUsed, nil
}

func readJoined(path string) ([]joinedRow, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read joined: %w", err)
	}
	var rows []joinedRow
	if err := json.Unmarshal(b, &rows); err != nil {
		return nil, fmt.Errorf("parse joined: %w", err)
	}
	return rows, nil
}

// ---- 통계 유틸 ----

func median(xs []float64) float64 {
	n := len(xs)
	if n == 0 {
		return 0
	}
	cp := append([]float64(nil), xs...)
	sort.Float64s(cp)
	if n%2 == 1 {
		return cp[n/2]
	}
	return 0.5 * (cp[n/2-1] + cp[n/2])
}

func quantileSorted(sorted []float64, p float64) float64 {
	n := len(sorted)
	if n == 0 {
		return math.NaN()
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 1 {
		return sorted[n-1]
	}
	pos := (float64(n - 1)) * p
	l := int(math.Floor(pos))
	u := int(math.Ceil(pos))
	if l == u {
		return sorted[l]
	}
	frac := pos - float64(l)
	return sorted[l]*(1-frac) + sorted[u]*frac
}

func invECDF(sortedX []float64, q float64) float64 {
	n := len(sortedX)
	if n == 0 {
		return math.NaN()
	}
	if q <= 0 {
		return sortedX[0]
	}
	if q >= 1 {
		return sortedX[n-1]
	}
	pos := q * float64(n-1)
	l := int(math.Floor(pos))
	r := int(math.Ceil(pos))
	if l == r {
		return sortedX[l]
	}
	t := pos - float64(l)
	return sortedX[l] + t*(sortedX[r]-sortedX[l])
}

func toZ(x, q10, q90 float64) float64 {
	if x < q10 {
		x = q10
	} else if x > q90 {
		x = q90
	}
	if q90 == q10 {
		return 0
	}
	return (x - q10) / (q90 - q10)
}
