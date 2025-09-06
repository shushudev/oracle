// consumer/solar_scheduler.go

package consumer

import (
	"context"
	"log"
	"sync"
	"time"
)

var solarSchedOnce sync.Once

// 외부에서 한 번만 호출하세요(예: vote_reward.go의 StartVMemberRewardConsumer 시작 직전)
func StartSolarAverageScheduler(ctx context.Context) {
	solarSchedOnce.Do(func() {
		go runSolarAverageScheduler(ctx)
	})
}

func runSolarAverageScheduler(parent context.Context) {
	// 1) 즉시 1회 로딩 (타임아웃 45s)
	loadWithTimeout(parent, 45*time.Second)

	// 2) 매 정시 + 지연(offset)마다 반복 로딩
	const offset = 10 * time.Minute // KMA 반영 지연 감안: 정시 + 10분에 조회 권장
	for {
		wait := untilNextKSTTopOfHourPlus(offset)
		select {
		case <-time.After(wait):
			loadWithTimeout(parent, 45*time.Second)
		case <-parent.Done():
			return
		}
	}
}

func loadWithTimeout(parent context.Context, d time.Duration) {
	ctx, cancel := context.WithTimeout(parent, d)
	defer cancel()
	if err := SaveSolarRadiationJSON(ctx); err != nil {
		log.Printf("[ERROR] Radiation data update failed: %v", err)
	} else {
		log.Printf("[UPDATE] Success to Update")
	}
}

func untilNextKSTTopOfHourPlus(offset time.Duration) time.Duration {
	now := time.Now().In(kst())
	next := now.Truncate(time.Hour).Add(time.Hour).Add(offset)
	return next.Sub(now)
}
