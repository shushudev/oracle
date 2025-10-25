-- 003_turn_finalize.sql
-- 턴(1REC) 종료 시 결과 및 vote reset 원장(ledger) 스키마

-- 1) 턴 결과 (idempotency: turn_id PK)
CREATE TABLE IF NOT EXISTS turn_result (
  turn_id     TEXT PRIMARY KEY,
  fullnode_id TEXT NOT NULL,
  creator     TEXT NOT NULL,
  weight      DOUBLE PRECISION NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 2) vote reset 원장(ledger)
--    vote_counter가 (address, last_time, count) 구조이므로
--    last_time의 변경 내역도 함께 저장한다.
CREATE TABLE IF NOT EXISTS vote_counter_ledger (
  turn_id           TEXT NOT NULL,
  address           TEXT NOT NULL,
  before_count      DOUBLE PRECISION NOT NULL,
  after_count       DOUBLE PRECISION NOT NULL,
  delta             DOUBLE PRECISION NOT NULL,
  before_last_time  TIMESTAMPTZ,
  after_last_time   TIMESTAMPTZ,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (turn_id, address)
);

-- 3) 조회 가속 인덱스
CREATE INDEX IF NOT EXISTS idx_vcl_turn ON vote_counter_ledger (turn_id);
CREATE INDEX IF NOT EXISTS idx_vcl_addr ON vote_counter_ledger (address);
