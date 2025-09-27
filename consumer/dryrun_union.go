// Test Purpose Code
package consumer

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"time"

	dbx "oracle/db"
)

func DryRunUnionRoulette(db *sql.DB, baseContribs []Contributor, fullnodeID, seedTag string, beta float64) {
	// 1) 합집합 구성
	voteAddrs, err := FetchVoteAddresses(db)
	if err != nil {
		fmt.Println("[DryRunUnion] FetchVoteAddresses err:", err)
		voteAddrs = map[string]struct{}{}
	}
	candidates := UnionContributorsWithVoteOnly(baseContribs, voteAddrs)
	if len(candidates) == 0 {
		fmt.Println("[DryRunUnion] no candidates")
		return
	}

	// 2) 주소/점수 조회
	addrs := make([]string, 0, len(candidates))
	for _, c := range candidates {
		if c.Address != "" {
			addrs = append(addrs, c.Address)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	scoreMap, err := dbx.GetVoteCountsByAddresses(ctx, db, addrs)
	cancel()
	if err != nil {
		fmt.Println("[DryRunUnion] GetVoteCountsByAddresses err:", err)
		scoreMap = map[string]float64{}
	}

	// 3) 기존 DryRunRoulette 재사용
	seedMaterial := fmt.Sprintf("%s:%s", fullnodeID, seedTag)
	w, ww, pp := DryRunRoulette(candidates, scoreMap, beta, seedMaterial)
	fmt.Printf("[DryRunUnion] winner=%s w=%.6f P=%.6f candidates=%d\n", w, ww, pp, len(candidates))

	// (선택) 주소 정렬·표시
	sort.Strings(addrs)
	fmt.Println("[DryRunUnion] addrs:", addrs)
}
