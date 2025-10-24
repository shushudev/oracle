// oracle/consumer/candidate_union.go
package consumer

import (
	"database/sql"
)

// DB의 vote_counter에서 count > 0 인 주소들을 모두 가져옵니다.
// - 이들은 이번 턴에 기여(에너지) 데이터가 없어도 'vote-only' 후보로 포함되어야 합니다.
func FetchVoteAddresses(db *sql.DB) (map[string]struct{}, error) {
	rows, err := db.Query(`SELECT address FROM vote_counter WHERE count > 0`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	addrs := make(map[string]struct{}, 1024)
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			return nil, err
		}
		if a == "" {
			continue
		}
		addrs[a] = struct{}{}
	}
	return addrs, rows.Err()
}

// 기존 "기여자 목록"과 "vote-only 주소"를 합집합으로 병합합니다.
// - 기여자에 이미 있는 주소는 그대로 둡니다.
// - 기여자에 없는 vote-only 주소는 EnergyKwh="0"인 가상의 Contributor로 추가합니다.
// - 중복 주소는 자동 제거됩니다.
func UnionContributorsWithVoteOnly(contribs []Contributor, voteAddrs map[string]struct{}) []Contributor {
	seen := make(map[string]struct{}, len(contribs))
	out := make([]Contributor, 0, len(contribs)+len(voteAddrs))

	// 1) 기여자 먼저 넣고, seen 처리
	for _, c := range contribs {
		if c.Address == "" {
			continue
		}
		if _, dup := seen[c.Address]; dup {
			continue
		}
		out = append(out, c)
		seen[c.Address] = struct{}{}
		// 기여자에 이미 있으면 vote-only 세트에서 제거 (중복 방지)
		delete(voteAddrs, c.Address)
	}

	// 2) 남은 vote-only 주소는 EnergyKwh=0으로 추가
	for addr := range voteAddrs {
		out = append(out, Contributor{
			Address:   addr,
			EnergyKwh: "0",
		})
	}

	return out
}
