package config

var (
	// ------------------ Kafka ------------------
	KafkaBrokers = []string{"Kafka00Service:9092", "Kafka01Service:9092", "Kafka02Service:9092"}

	// Kafka 토픽 Consumer
	TopicDeviceIdToAddressRequest = "device-address-request-topic"
	TopicRequestMemberCount       = "request-user-count-topic"
	TopicRequestLocation          = "request-location-topic"    // 풀노드에서 위치정보 전송
	TopicRequestVMemberReward     = "request-vote-member-topic" // 풀노드 -> 오라클 (서명자 보상 결과 전송)
	TopicTxHash                   = "tx-hash-topic"
	TopicRequestTxHash            = "request-tx-hash-topic"
	TopicContributors             = "send-contributors" // 풀노드 -> 기여자 리스트 전송

	// Kafka Topic Producer
	TopicDeviceIdToAddressProducer = "device-address-topic"
	TopicVoteMemberProducer        = "user-count-topic"
	TopicResultLocationProducer    = "result-location-topic"
	TopicCreateAccountProducer     = "create-address-topic"
	TopicResultVMemberReward       = "result-vote-member-reward" // 오라클 -> 풀노드 (서명자 리스트 받기)
	TopicResultTxhashProducer      = "result-tx-hash-topic"      // 오라클 -> 라이트 노드
	TopicRECPrice                  = "rec-price-topic"           // 오라클 -> 라이트 노드
	TopicBlockCreator              = "block-creator"             // 블록 생성사 선출 결과

	// Kafka Group
	GroupVote              = "vote-member-group"
	GroupDeviceIdToAddress = "device-address-group"
	GroupVoteListen        = "vote-member-listen-group"

	// ------------------ Database ------------------
	Dsn           = "postgres://capstone2:block1234@postgres:5432/user_info?sslmode=disable"
	LightNodeUser = 0

	// ------------------ KMA API & 경로 설정 ------------------
	KMAAuthKey        = "ttruCV71S-aa7gle9ZvmFA"                                // 기상청 API 키
	KMAAPIURL         = "https://apihub.kma.go.kr/api/typ01/url/kma_sfctm2.php" // 기상청 API URL
	KMAStationDefault = ""                                                      // 기본 전체 지점 조회
	KMABackoffHours   = 3                                                       // 직전 정시부터 최대 3시간 과거 자동 탐색
	KMAOutputPath     = "solar_radiation.json"                                  // 원본 관측치 저장 경로

	// ------------------ 권역 기반 산출용 경로 ------------------
	KMAStationsPath     = "solar_stations.json"             // 지점 메타 정보(주소/지점명)
	KMAJoinedOutPath    = "solar_radiation_joined.json"     // 관측치 + 메타 조인 결과
	KMARegionAggOutPath = "solar_radiation_region_agg.json" // 권역별 집계 결과
	KMAStationRegionOut = "solar_station_regions.json"      // 지점→권역 매핑 테이블
	KMAAverage          float64                             // 실시간 전국 원시 일사량 평균 값 저장

	// 권역 스킴 (예: "Sido17" = 시·도 단위, "Macro6" = 수도권/강원권/충청권/호남권/영남권/제주권)
	RegionScheme = "Sido17"

	// ---------------- 기본 보상에 대한 수식 사용 ====================
	EnableInverse = true
	Bscale        = 1.0
	R_0           float64
	Q_L           = 0.10
	Q_H           = 0.90

	RequestedRegions = map[string]bool{
		"서울특별시":   true,
		"경기도":     true,
		"제주특별시":   true,
		"대구/경상북도": true,
		"대전/충청남도": true,
		"충청북도":    true,
		"부산/경상남도": true,
		"광주/전라남도": true,
		"전라북도":    true,
		"강원도":     true,
	}
)
