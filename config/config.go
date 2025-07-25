package config

var (
	// Kafka 브로커 IP 및 포트
	KafkaBrokers = []string{"Kafka00Service:9092"}

	// Kafka 토픽 Consumer
	TopicDeviceIdToAddress = "device-address-topic"
	// Kafka Topic Producer
	TopicDeviceIdResponse = "device-address-response"
	TopicVoteMember       = "user-count-topic"

	// Kafka Group
	GroupVote              = "vote-member-group"
	GroupDeviceIdToAddress = "device-address-group"
	GroupVoteListen        = "vote-member-listen-group"
	// database
	Dsn = "postgres://capstone2:block1234@postgres:5432/user_info?sslmode=disable"
)
