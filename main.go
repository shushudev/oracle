package main

import (
	"log"
	"net/http"
	api "oracle/connect"
	"oracle/consumer"
	"oracle/db"
	"oracle/producer"
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

	go consumer.StartRequestVoteMemberConsumer(database)
	go consumer.StartLocationConsumer(database, locationWriter)
	go consumer.StartMappingConsumer(database, mappingWriter)

	log.Println("Server running on :3001")
	log.Fatal(http.ListenAndServe(":3001", nil))
	select {}
	// defer voteWriter.Close() // 필요 시 종료 시점에 닫기
	// defer mappingWriter.Close()

}
