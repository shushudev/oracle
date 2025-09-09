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
	locationWriter := producer.NewLocationWriter()
	accountCreateWriter := producer.NewAccounCreatetWriter()
	vmMemberWriter := producer.InitRewardProducer()
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

	go consumer.StartMappingConsumer(database, writer)               // device Id -> address
	go producer.StartRequestVoteMemberConsumer(database, writer)     // 유권자 수 전송
	go consumer.StartLocationConsumer(database, locationWriter)      // 위치정보 요청
	go consumer.StartVMemberRewardConsumer(database, vmMemberWriter) // 서명자 보상
	go consumer.StartTxHashConsumer(database)                        // tx hash값 저장
	go consumer.StartRequestTxHashConsumer(database, txHashWriter)
	go consumer.StartCollateralsConsumer(database, writer) // REC 등록, 담보 예치
	go consumer.StartBurnConsumer(database, writer)        // 소각 후 REC 반환
	log.Println("Server running on :3001")
	log.Fatal(http.ListenAndServe(":3001", nil))
	select {}
}
