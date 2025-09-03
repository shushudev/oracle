package config

var (
	// Kafka 브로커 IP 및 포트
	KafkaBrokers = []string{"Kafka00Service:9092", "Kafka01Service:9092", "Kafka02Service:9092"}

	// Kafka 토픽 Consumer
	TopicDeviceIdToAddressRequest = "device-address-request-topic"
	TopicRequestMemberCount       = "request-user-count-topic"
	TopicRequestLocation          = "request-location-topic"    // 풀노드에서 위치정보 전송
	TopicRequestVMemberReward     = "request-vote-member-topic" // 풀노드 -> 오라클 (서명자 보상 결과 전송)
	TopicTxHash                   = "tx-hash-topic"
	TopicRequestTxHash            = "request-tx-hash-topic"
	// Kafka Topic Producer
	TopicDeviceIdToAddressProducer = "device-address-topic"
	TopicVoteMemberProducer        = "user-count-topic"
	TopicResultLocationProducer    = "result-location-topic"
	TopicCreateAccountProducer     = "create-address-topic"
	TopicResultVMemberReward       = "result-vote-member-reward" // 오라클 -> 풀노드 (서명자 리스트 받기)
	TopicResultTxhashProducer      = "result-tx-hash-topic"      // 오라클 -> 라이트 노드
	// Kafka Group
	GroupVote              = "vote-member-group"
	GroupDeviceIdToAddress = "device-address-group"
	GroupVoteListen        = "vote-member-listen-group"

	// database
	Dsn           = "postgres://capstone2:block1234@postgres:5432/user_info?sslmode=disable"
	LightNodeUser = 0

	KMAAuthKey        = "ttruCV71S-aa7gle9ZvmFA"
	KMAAPIURL         = "https://apihub.kma.go.kr/api/typ01/url/kma_sfctm2.php"
	KMAStationDefault = ""                     // 기본 전체 지점 조회
	KMABackoffHours   = 3                      // 직전 정시부터 최대 3시간 과거 자동 탐색
	KMAOutputPath     = "solar_radiation.json" // 기본 출력 파일
	KMAAverage        float64
)
