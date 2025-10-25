package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// 턴별 집계
	FairPenalizedGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{Name: "fair_penalized_candidates", Help: "Number of penalized candidates in the turn"},
	)
	FairMaxPenaltyGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{Name: "fair_max_penalty_factor", Help: "The strongest penalty factor applied in the turn (min gamma^R)"},
	)
	FairCandidatesGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{Name: "fair_candidates_total", Help: "Number of candidates in the turn"},
	)
	FairPcapAppliedGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{Name: "fair_pcap_applied", Help: "1 if P-cap triggered in the turn; otherwise 0"},
	)
	// 승자 카운터 (라벨: creator)
	WinnerCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "block_winner_total", Help: "Total wins per creator"},
		[]string{"creator"},
	)
)

func InitAndServe(addr string) error {
	prometheus.MustRegister(FairPenalizedGauge, FairMaxPenaltyGauge, FairCandidatesGauge, FairPcapAppliedGauge, WinnerCounter)
	http.Handle("/metrics", promhttp.Handler())
	// 별도 HTTP 서버 (블로킹하지 않도록 상위에서 고루틴으로 호출 권장)
	return http.ListenAndServe(addr, nil)
}
