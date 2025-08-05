package db

import (
	"database/sql"
	"log"
	"oracle/config"

	_ "github.com/lib/pq"
)

func ConnectDB() *sql.DB {

	db, err := sql.Open("postgres", config.Dsn)
	if err != nil {
		log.Fatalf("DB 연결 실패: %v", err)
	}

	// 연결 확인
	if err := db.Ping(); err != nil {
		log.Fatalf("DB Ping 실패: %v", err)
	}

	log.Println("PostgreSQL 연결 성공")
	return db
}
