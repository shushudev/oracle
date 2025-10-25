package main

import (
	"log"
	"oracle/config"
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
	accountCreateWriter := producer.NewAccounCreatetWriter()
	txHashWriter := producer.NewTxHashWriter()

	writer, err := producer.NewSaramaProducer(config.KafkaBrokers)
	if err != nil {
		panic(err)
	}
	defer writer.Close()

	go producer.StartUserMonitor(database, writer)

	// HTTP 서버: /connect API 등록
	http.HandleFunc("/connect", func(w http.ResponseWriter, r *http.Request) {
		enableCORS(w)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		api.ConnectHandler(database, accountCreateWriter)(w, r)
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

	go consumer.StartMappingConsumer(database, writer)           // device Id -> address
	go producer.StartRequestVoteMemberConsumer(database, writer) // 유권자 수 전송
	go consumer.StartVMemberRewardConsumer(database)             // 서명자 보상
	go consumer.StartTxHashConsumer(database)                    // tx hash값 저장
	go consumer.StartMappingConsumer(database, writer)           // device Id -> address
	go producer.StartRequestVoteMemberConsumer(database, writer) // 유권자 수 전송
	go consumer.StartVMemberRewardConsumer(database)             // 서명자 보상
	go consumer.StartTxHashConsumer(database)                    // tx hash값 저장
	go consumer.StartRequestTxHashConsumer(database, txHashWriter)
	// go producer.StartOracleProducer(writer)
	go consumer.StartBlockCreatorConsumer(database, writer)
	log.Println("Server running on :3001")
	log.Fatal(http.ListenAndServe(":3001", nil))
	select {}
}
