package main

import (
	"log"
	api "oracle/connect"

	"oracle/consumer"
	"oracle/db"
	"oracle/producer"

	"net/http"
)

func enableCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
}

func main() {
	database := db.ConnectDB()
	mappingWriter := producer.NewMappingWriter()
	voteWriter := producer.NewVoteMemberWriter()
	locationWriter := producer.NewLocationWriter()

	go producer.StartUserMonitor(database, voteWriter)

	// HTTP 서버: /connect API 등록
	http.HandleFunc("/connect", func(w http.ResponseWriter, r *http.Request) {
		enableCORS(w)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		api.ConnectHandler(database, voteWriter)(w, r)
	})

	// HTTP 서버: /verify API 등록
	http.HandleFunc("/verify", func(w http.ResponseWriter, r *http.Request) {
		enableCORS(w)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		api.VerifyHandler(database)(w, r) // VerifyHandler는 connect/verify.go에 구현
	})

	// ✅ 세 개의 Kafka Consumer 실행
	go consumer.StartMappingConsumer(database, mappingWriter)
	go consumer.StartRequestVoteMemberConsumer(database)
	go consumer.StartLocationConsumer(database, locationWriter)

	log.Println("Server running on :3001")
	log.Fatal(http.ListenAndServe(":3001", nil))
	select {}
}
