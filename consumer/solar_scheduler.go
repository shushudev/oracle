package consumer

import (
	"context"
	"log"
	"time"
)

var schedulerOnce struct{ done bool } // 기존에 sync.Once가 있으면 그걸 그대로 쓰세요.

// 다음 “정각+10분(HH:10:00)”까지 대기시간 계산
func untilNextTopPlus10(now time.Time) time.Duration {
	now = now.In(kst())
	y, m, d := now.Date()
	target := time.Date(y, m, d, now.Hour(), 10, 0, 0, now.Location())
	if !now.Before(target) {
		// 이미 HH:10 이후면 다음 시간의 :10으로
		target = target.Add(1 * time.Hour)
	}
	return time.Until(target)
}

func StartSolarAverageScheduler(ctx context.Context) {
	// sync.Once가 이미 있다면 그대로 쓰세요. (예: schedulerOnce.Do(func(){...}))
	if schedulerOnce.done {
		return
	}
	schedulerOnce.done = true

	// 1) “즉시 실행”을 하지 말고, 다음 HH:10까지 기다린다.
	sleep := untilNextTopPlus10(time.Now())
	log.Printf("[SCHED] first run aligned: sleeping %v until next :10", sleep)

	timer := time.NewTimer(sleep)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		log.Printf("[SCHED] canceled before first run")
		return
	case <-timer.C:
	}

	// 2) 첫 실행: 여기서부터 1시간 주기 (:10 기준 고정)
	run := func() {
		// 내부에서 SaveSolarRadiationJSON이 tm=직전정시로 파이프라인 실행
		if err := SaveSolarRadiationJSON(ctx); err != nil {
			log.Printf("[SCHED] pipeline error: %v", err)
		}
	}

	run() // 첫 실행

	ticker := time.NewTicker(1 * time.Hour) // HH:10 기준으로 1시간마다 실행
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[SCHED] context canceled")
			return
		case <-ticker.C:
			run()
		}
	}
}
