// oracle/consumer/block_creator.go
package consumer

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"time"

	"oracle/config"
	dbx "oracle/db"
	"oracle/metrics"

	"github.com/IBM/sarama"
	"github.com/lib/pq"
)

func debugRouletteOn() bool { return os.Getenv("DEBUG_ROULETTE") == "1" }

// ---- 메시지 스키마 ----
type Contributor struct {
	Address   string `json:"address,omitempty"`
	EnergyKwh string `json:"energy_kwh,omitempty"`
}
type BlockContributorMsg struct {
	FullnodeID   string        `json:"fullnode_id"`
	Contributors []Contributor `json:"contributors"`
}
type BlockCreatorMsg struct {
	Creator      string  `json:"creator"`
	Contribution float64 `json:"contribution"` // 디버그용: 최종 가중치(=w_i)
	FullnodeID   string  `json:"fullnode_id"`
}

// StartBlockCreatorConsumer
// - TopicContributors를 구독
// - w_i = β·x_i + (1-β)·r_i 기반 룰렛휠로 1명 선발
// - 결과를 TopicBlockCreator로 송신
func StartBlockCreatorConsumer(db *sql.DB, producer sarama.SyncProducer) error {
	// [SCHEMA BOOTSTRAP] main.go를 건드리지 않고 여기서 1회 보장

	go func() {
		if err := metrics.InitAndServe(":9090"); err != nil {
			fmt.Println("[Metrics] server error:", err)
		}
	}()
	ctxIdx, cancelIdx := context.WithTimeout(context.Background(), 5*time.Second)
	// turn_id 정수 시퀀스를 쓰는지 여부에 맞게 true/false
	_ = dbx.EnsureFairnessIndexes(ctxIdx, db, true)
	cancelIdx()
	ctxInit, cancelInit := context.WithTimeout(context.Background(), 5*time.Second)
	if err := dbx.BootstrapTurnTables(ctxInit, db); err != nil {
		cancelInit()
		return fmt.Errorf("schema bootstrap failed: %w", err)
	}
	cancelInit()

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_1_0_0

	cons, err := sarama.NewConsumer(config.KafkaBrokers, cfg)
	if err != nil {
		return err
	}

	const partition = int32(0)
	pc, err := cons.ConsumePartition(config.TopicContributors, partition, sarama.OffsetNewest)
	if err != nil {
		_ = cons.Close()
		return err
	}

	go func() {
		defer func() { _ = pc.Close(); _ = cons.Close() }()

		for m := range pc.Messages() {
			if m == nil || len(m.Value) == 0 {
				continue
			}

			var data BlockContributorMsg
			if err := json.Unmarshal(m.Value, &data); err != nil {
				fmt.Printf("[BlockCreator] payload parse fail: %v\n", err)
				continue
			}

			voteAddrs, err := FetchVoteAddresses(db) // vote_counter에서 count>0 주소
			if err != nil {
				fmt.Println("FetchVoteAddresses err:", err)
				// 최소 기능 유지: voteAddrs가 nil이면 빈 맵으로 대체
				voteAddrs = map[string]struct{}{}
			}
			eligibleContributors := UnionContributorsWithVoteOnly(data.Contributors, voteAddrs)

			// ★ 합집합 기준으로 검증(빈 기여자 선탈락 금지)
			if len(eligibleContributors) == 0 {
				fmt.Println("[BlockCreator] no candidates (contributors ∪ vote-only empty)")
				continue
			}

			// 1) 주소 목록 (합집합 기준)
			addrs := make([]string, 0, len(eligibleContributors))
			for _, c := range eligibleContributors {
				if c.Address != "" {
					addrs = append(addrs, c.Address)
				}
			}
			if len(addrs) == 0 {
				fmt.Println("[BlockCreator] no valid addresses")
				continue
			}

			// 2) vote_counter에서 누적 점수 조회
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			scoreMap, err := dbx.GetVoteCountsByAddresses(ctx, db, addrs)
			cancel()
			if err != nil {
				fmt.Printf("[BlockCreator] vote_counter query failed: %v\n", err)
				scoreMap = map[string]float64{}
			}

			// 3) x_i = e_i/E, r_i = count_i / sum(count)
			var E float64
			energy := make(map[string]float64, len(addrs)) // 단위 상관없음(합으로만 사용)
			for _, c := range eligibleContributors {
				if c.Address == "" {
					continue
				}
				ekwh, _ := strconv.ParseFloat(c.EnergyKwh, 64) // 실패 시 0
				if ekwh < 0 {
					ekwh = 0
				}
				energy[c.Address] = ekwh
				E += ekwh
			}
			var S float64
			for _, a := range addrs {
				S += scoreMap[a]
			}
			if E == 0 {
				fmt.Println("[BlockCreator] note: E==0 (no energy this turn)")
			}
			if S == 0 {
				fmt.Println("[BlockCreator] note: S==0 (no votes among union candidates)")
			}
			x := make(map[string]float64, len(addrs))
			rv := make(map[string]float64, len(addrs))
			for _, a := range addrs {
				if E > 0 {
					x[a] = energy[a] / E
				} else {
					x[a] = 0
				}
				if S > 0 {
					rv[a] = scoreMap[a] / S
				} else {
					rv[a] = 0
				}
			}

			// 4) w_i = β·x_i + (1-β)·r_i + ε  (룰렛휠)
			// [SAFE] eps/beta를 설정에서 읽고, beta 클램프
			eps := config.RouletteEps
			if eps <= 0 {
				eps = 1e-12
			}
			beta := config.BlockSelectBeta
			if beta < 0 {
				beta = 0
			}
			if beta > 1 {
				beta = 1
			}

			type pair struct {
				addr string
				w    float64
				p    float64
				f    float64
			}
			ps := make([]pair, 0, len(addrs))
			var W float64
			for _, a := range addrs {
				w := beta*x[a] + (1.0-beta)*rv[a]
				if w < 0 {
					w = 0
				}
				// (선택) NaN/Inf 방어
				if math.IsNaN(w) || math.IsInf(w, 0) {
					w = 0
				}
				w += eps
				ps = append(ps, pair{addr: a, w: w})
				W += w
			}
			if len(ps) == 0 {
				fmt.Println("[BlockCreator] no candidates after weighting")
				continue
			}

			// [SAFE] 만약 모든 w가 0+eps 수준이라 W가 eps*len(addrs)에 매우 가깝더라도 정상 동작.
			// 극히 드문 케이스로 W<=0이면 균등 분포로 대체.
			if W <= 0 {
				fmt.Println("[BlockCreator] note: W<=0 -> fallback to uniform weights")
				ps = ps[:0]
				for _, a := range addrs {
					w := 1.0 // 균등
					ps = append(ps, pair{addr: a, w: w})
				}
				W = float64(len(addrs))
			}

			var stats map[string]struct {
				WinsInWindow int
				ExceedTurnID sql.NullInt64
			}

			if config.FairFeatureOn {
				// ---- 2-1) 현재 턴 ID 결정 ----
				// Kafka 파티션 오프셋을 "턴 번호"로 쓰면 일관된 BIGINT 시퀀스를 얻을 수 있음.
				// (주의) turn_result.turn_id 가 BIGINT 타입이라고 가정.
				// 만약 turn_id가 TEXT라면, fetchWinStatsForWindow를 created_at 기반 쿼리로 바꿔야 함.
				currentTurn := int64(m.Offset) // 필요시 +1 해도 무방
				ctxFair, cancelFair := context.WithTimeout(context.Background(), 2*time.Second)
				stats, err = fetchWinStatsForWindow(ctxFair, db, currentTurn, config.FairWinWindowN, config.FairWinCapM, addrs)
				cancelFair()
				if err != nil {
					fmt.Println("[Fairness] fetchWinStats error:", err)
				} else {
					// addr -> ps index 매핑 (ps에서 w를 바로 업데이트하기 위함)
					idx := make(map[string]int, len(ps))
					for i := range ps {
						idx[ps[i].addr] = i
					}

					// ---- 2-3) 패널티 적용 (ramp/fixed) ----
					for a, s := range stats {
						// 윈도우 내 승리 횟수가 M 초과 & (M+1)번째 최신 승리 턴 존재
						if s.WinsInWindow > config.FairWinCapM && s.ExceedTurnID.Valid {
							exceedTurn := s.ExceedTurnID.Int64
							// R = 남은 패널티 턴수 = K - (currentTurn - exceedTurn)
							R := config.FairSoftK - int(currentTurn-exceedTurn)
							if R > 0 {
								if i, ok := idx[a]; ok {
									switch config.FairSoftMode {
									case "fixed":
										ps[i].w *= config.FairSoftGamma
									default: // "ramp"
										ps[i].w *= math.Pow(config.FairSoftGamma, float64(R))
									}
								}
							}
						}
					}

					// ---- 2-4) 패널티 적용 후 W 재계산 ----
					W = 0
					for i := range ps {
						// 수치 안정성 보호
						if ps[i].w < 0 || math.IsNaN(ps[i].w) || math.IsInf(ps[i].w, 0) {
							ps[i].w = 0
						}
						W += ps[i].w
					}
					if W <= 0 {
						// 만약 모든 가중치가 0이 되었다면 균등 분포로 복구
						for i := range ps {
							ps[i].w = 1.0
						}
						W = float64(len(ps))
						fmt.Println("[Fairness] note: all-zero after penalty -> uniform fallback")
					}
				}
			}
			// ============================================================
			// 5) P_i, F_i 계산 (주소 정렬로 재현성: map 반복 순서 제거)
			sort.Slice(ps, func(i, j int) bool { return ps[i].addr < ps[j].addr })
			acc := 0.0
			for i := range ps {
				if W > 0 {
					ps[i].p = ps[i].w / W
				} else {
					ps[i].p = 0
				}
				acc += ps[i].p
				ps[i].f = acc
			}
			ps[len(ps)-1].f = 1.0 // 수치오차 보호

			if debugRouletteOn() {
				fmt.Println("[Roulette] ===== Candidate Table =====")
				fmt.Printf("[Roulette] Beta=%.3f  E=%.6f  S=%.6f  (eps=1e-12)\n", beta, E, S)
				fmt.Printf("[Roulette] %-44s | %10s %10s %10s %12s %12s %12s\n",
					"address", "e_i", "x_i", "r_i", "w_i", "P_i", "F_i")
				fmt.Println("[Roulette] -----------------------------------------------------------------------------------------------")
				for _, row := range ps {
					a := row.addr
					ei := energy[a] // 입력 에너지(단위 무관)
					xi := x[a]      // 발전 참여도
					ri := rv[a]     // 서명 참여도
					wi := row.w     // 최종 가중치
					Pi := row.p     // 최종 확률
					Fi := row.f     // 누적 경계
					fmt.Printf("[Roulette] %-44s | %10.4f %10.6f %10.6f %12.8f %12.8f %12.8f\n",
						a, ei, xi, ri, wi, Pi, Fi)
				}
				fmt.Println("[Roulette] ============================================================")
			}
			pcapTriggered := false

			if config.EnablePCap {
				// 1) 캡 적용
				if pcapTriggered {
					metrics.FairPcapAppliedGauge.Set(1)
				} else {
					metrics.FairPcapAppliedGauge.Set(0)
				}
				capped := make([]bool, len(ps))
				sumCapped := 0.0
				sumUncapped := 0.0
				for i := range ps {
					if ps[i].p > config.Pcap {
						sumCapped += config.Pcap
						capped[i] = true
					} else {
						sumUncapped += ps[i].p
					}
				}
				if sumCapped > 0 && sumUncapped > 0 {
					// 2) 잔여 확률 재분배
					leftover := 1.0 - sumCapped
					for i := range ps {
						if capped[i] {
							ps[i].p = config.Pcap
						} else {
							// 비례 재분배
							ps[i].p = (ps[i].p / sumUncapped) * leftover
						}
					}
					// 3) F_i 재계산
					acc = 0.0
					for i := range ps {
						acc += ps[i].p
						ps[i].f = acc
					}
					ps[len(ps)-1].f = 1.0
				}
				// 극단 케이스: 모두 Pcap 이하라면 아무 변화 없음
			}
			// 6) 재현 가능한 난수 시드: FullnodeID + topic/partition/offset
			seedMaterial := fmt.Sprintf("%s:%s:%d:%d", data.FullnodeID, m.Topic, m.Partition, m.Offset)
			sum := sha256.Sum256([]byte(seedMaterial))
			seed := int64(binary.LittleEndian.Uint64(sum[:8]))
			u := rand.New(rand.NewSource(seed)).Float64()

			// 7) 최초 F_i >= u 인 구간의 주소가 당첨
			winner := ps[len(ps)-1].addr
			winnerW := ps[len(ps)-1].w
			for _, v := range ps {
				if u <= v.f {
					winner = v.addr
					winnerW = v.w
					break
				}
			}
			penalized := 0
			maxPenalty := 1.0
			for _, a := range addrs {
				s, ok := stats[a] // fetchWinStatsForWindow 결과(이미 위에서 구함)
				if ok && s.WinsInWindow > config.FairWinCapM && s.ExceedTurnID.Valid {
					R := config.FairSoftK - int(int64(m.Offset)-s.ExceedTurnID.Int64)
					if R > 0 {
						penalized++
						// ramp 기준의 이론상 penalty 값 (로그용)
						p := config.FairSoftGamma
						if config.FairSoftMode != "fixed" {
							p = math.Pow(config.FairSoftGamma, float64(R))
						}
						if p < maxPenalty {
							maxPenalty = p
						}
					}
				}
			}
			fmt.Printf("[FairnessSummary] turn=%d fullnode=%s winner=%s penalized=%d maxPenalty=%.3f candidates=%d\n",
				m.Offset, data.FullnodeID, winner, penalized, maxPenalty, len(addrs))
			if config.FairFeatureOn && stats != nil {
				penalized := 0
				maxPenalty := 1.0
				currentTurn := int64(m.Offset)
				for _, a := range addrs {
					if s, ok := stats[a]; ok && s.WinsInWindow > config.FairWinCapM && s.ExceedTurnID.Valid {
						R := config.FairSoftK - int(currentTurn-s.ExceedTurnID.Int64)
						if R > 0 {
							penalized++
							p := config.FairSoftGamma
							if config.FairSoftMode != "fixed" {
								p = math.Pow(config.FairSoftGamma, float64(R))
							}
							if p < maxPenalty {
								maxPenalty = p
							}
						}
					}
				}
				metrics.FairPenalizedGauge.Set(float64(penalized))
				metrics.FairMaxPenaltyGauge.Set(maxPenalty)
				metrics.FairCandidatesGauge.Set(float64(len(addrs)))
			}
			msg := BlockCreatorMsg{
				Creator:      winner,
				Contribution: winnerW,
				FullnodeID:   data.FullnodeID,
			}
			payload, err := json.Marshal(msg)
			if err != nil {
				fmt.Printf("[BlockCreator] marshal failed: %v\n", err)
				continue
			}
			out := &sarama.ProducerMessage{
				Topic: config.TopicBlockCreator,
				Value: sarama.ByteEncoder(payload),
			}
			_, _, err = producer.SendMessage(out)
			if err != nil {
				fmt.Printf("[BlockCreator] send failed: %v\n", err)
			} else {
				fmt.Printf("[BlockCreator] sent → creator=%s w=%.6f seed=%s fullnode=%s\n",
					msg.Creator, msg.Contribution, seedMaterial, msg.FullnodeID)

				// === [3단계 연결] 송신 성공 시 턴 종료 처리 ===
				turnID := int64(m.Offset) // Kafka offset을 turn_id로 사용(정수)
				ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
				err2 := dbx.FinalizeTurnNoResetTx(ctx2, db, turnID, data.FullnodeID, winner, winnerW)
				cancel2()
				if err2 != nil {
					fmt.Printf("[BlockCreator] finalize-turn failed: %v (turn_id=%d)\n", err2, turnID)
				} else {
					fmt.Printf("[BlockCreator] finalize-turn OK (turn_id=%d)\n", turnID)
				}
			}
		}
	}()

	return nil
}

func DryRunRoulette(contributors []Contributor, voteMap map[string]float64, beta float64, seedMaterial string) (string, float64, float64) {
	// 1) 주소 및 에너지 합
	addrs := make([]string, 0, len(contributors))
	energy := make(map[string]float64, len(contributors))
	var E float64
	for _, c := range contributors {
		if c.Address == "" {
			continue
		}
		addrs = append(addrs, c.Address)
		ekwh, _ := strconv.ParseFloat(c.EnergyKwh, 64)
		if ekwh < 0 {
			ekwh = 0
		}
		energy[c.Address] = ekwh // 단위 무관
		E += ekwh
	}
	if len(addrs) == 0 {
		fmt.Println("[DryRun] no valid contributors")
		return "", 0, 0
	}

	// 2) 점수 합
	var S float64
	for _, a := range addrs {
		S += voteMap[a]
	}

	if E == 0 {
		fmt.Println("[BlockCreator] note: E==0 (no energy this turn)")
	}
	if S == 0 {
		fmt.Println("[BlockCreator] note: S==0 (no votes among union candidates)")
	}
	// 3) x_i, r_i
	x := make(map[string]float64, len(addrs))
	rv := make(map[string]float64, len(addrs))
	for _, a := range addrs {
		if E > 0 {
			x[a] = energy[a] / E
		} else {
			x[a] = 0
		}
		if S > 0 {
			rv[a] = voteMap[a] / S
		} else {
			rv[a] = 0
		}
	}

	// 4) w_i = β·x_i + (1-β)·r_i + eps
	const eps = 1e-12
	if beta < 0 {
		beta = 0
	}
	if beta > 1 {
		beta = 1
	}
	type pair struct {
		addr string
		w    float64
		p    float64
		f    float64
	}
	ps := make([]pair, 0, len(addrs))
	var W float64
	for _, a := range addrs {
		w := beta*x[a] + (1.0-beta)*rv[a]
		if w < 0 {
			w = 0
		}
		w += eps
		ps = append(ps, pair{addr: a, w: w})
		W += w
	}

	// 정렬(재현성)
	sort.Slice(ps, func(i, j int) bool { return ps[i].addr < ps[j].addr })

	// P_i, F_i
	acc := 0.0
	for i := range ps {
		if W > 0 {
			ps[i].p = ps[i].w / W
		} else {
			ps[i].p = 0
		}
		acc += ps[i].p
		ps[i].f = acc
	}
	if len(ps) > 0 {
		ps[len(ps)-1].f = 1.0
	}

	// 디버그 표 출력
	fmt.Println("[DryRun] ===== Candidate Table =====")
	fmt.Printf("[DryRun] Beta=%.3f  E=%.6f  S=%.6f  (eps=1e-12)\n", beta, E, S)
	fmt.Printf("[DryRun] %-44s | %10s %10s %10s %12s %12s %12s\n",
		"address", "e_i", "x_i", "r_i", "w_i", "P_i", "F_i")
	fmt.Println("[DryRun] -----------------------------------------------------------------------------------------------")
	for _, row := range ps {
		a := row.addr
		ei := energy[a]
		xi := x[a]
		ri := rv[a]
		fmt.Printf("[DryRun] %-44s | %10.4f %10.6f %10.6f %12.8f %12.8f %12.8f\n",
			a, ei, xi, ri, row.w, row.p, row.f)
	}
	fmt.Println("[DryRun] ============================================================")

	// 난수 시드
	sum := sha256.Sum256([]byte(seedMaterial))
	seed := int64(binary.LittleEndian.Uint64(sum[:8]))
	u := rand.New(rand.NewSource(seed)).Float64()

	// 선택
	winner := ps[len(ps)-1].addr
	winnerW := ps[len(ps)-1].w
	winnerP := ps[len(ps)-1].p
	for _, v := range ps {
		if u <= v.f {
			winner = v.addr
			winnerW = v.w
			winnerP = v.p
			break
		}
	}

	fmt.Printf("[DryRun] WINNER=%s  w=%.8f  P=%.8f  seed=%s  u=%.8f\n",
		winner, winnerW, winnerP, seedMaterial, u)
	return winner, winnerW, winnerP
}

// currentTurn: 현재 턴 ID (int64), addrs: 후보 주소 배열
func fetchWinStatsForWindow(ctx context.Context, db *sql.DB, currentTurn int64, N, M int, addrs []string) (map[string]struct {
	WinsInWindow int
	ExceedTurnID sql.NullInt64
}, error) {
	q := `
WITH wins AS (
  SELECT creator, turn_id,
         ROW_NUMBER() OVER (PARTITION BY creator ORDER BY turn_id DESC) AS rn
    FROM turn_result
   WHERE turn_id > $1 - $2
     AND creator = ANY($3)
)
SELECT creator,
       COUNT(*) FILTER (WHERE rn <= $4) AS wins_in_window,
       MAX(CASE WHEN rn = $4 THEN turn_id END) AS exceed_turn_id
  FROM wins
 GROUP BY creator;
`
	rows, err := db.QueryContext(ctx, q, currentTurn, N, pq.Array(addrs), M+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]struct {
		WinsInWindow int
		ExceedTurnID sql.NullInt64
	})
	for rows.Next() {
		var creator string
		var wins int
		var ex sql.NullInt64
		if err := rows.Scan(&creator, &wins, &ex); err != nil {
			return nil, err
		}
		out[creator] = struct {
			WinsInWindow int
			ExceedTurnID sql.NullInt64
		}{WinsInWindow: wins, ExceedTurnID: ex}
	}
	return out, nil
}
