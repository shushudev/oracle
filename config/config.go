package config

var (
	// Kafka 브로커 IP 및 포트
	KafkaBrokers = []string{"Kafka00Service:9092", "Kafka01Service:9092", "Kafka02Service:9092"}

	// Kafka 토픽 Consumer
	TopicDeviceIdToAddressRequest = "device-address-request-topic"
	TopicRequestMemberCount       = "request-user-count-topic"
	TopicRequestLocation          = "request-location-topic" // 풀노드에서 위치정보 전송
	// Kafka Topic Producer
	TopicDeviceIdToAddressProducer = "device-address-topic"
	TopicVoteMemberProducer        = "user-count-topic"
	TopicResultLocationProducer    = "result-location-topic"
	TopicCreateAccountProducer     = "create-address-topic"
	// Kafka Group
	GroupVote              = "vote-member-group"
	GroupDeviceIdToAddress = "device-address-group"
	GroupVoteListen        = "vote-member-listen-group"
	// database
	Dsn = "postgres://capstone2:block1234@postgres:5432/user_info?sslmode=disable"
)
