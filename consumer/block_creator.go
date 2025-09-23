// oracle/consumer/block_creator.go
package consumer

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/IBM/sarama"

	"oracle/config"
	dbx "oracle/db"
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
			if len(data.Contributors) == 0 {
				fmt.Println("[BlockCreator] empty contributors")
				continue
			}

			// 1) 주소 목록
			addrs := make([]string, 0, len(data.Contributors))
			for _, c := range data.Contributors {
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
			for _, c := range data.Contributors {
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
			const eps = 1e-12
			beta := 0.7 // 기본값
			if config.BlockSelectBeta > 0 && config.BlockSelectBeta < 1 {
				beta = config.BlockSelectBeta
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
			if len(ps) == 0 {
				fmt.Println("[BlockCreator] no candidates after weighting")
				continue
			}

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

			// 8) 당첨자 송신
			msg := BlockCreatorMsg{
				Creator:      winner,
				Contribution: winnerW, // 최종 w_i
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
