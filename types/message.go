package types

// 요청 구조체 connect.go
type ConnectRequest struct {
	NodeID    string `json:"node_id"`
	DeviceID  string `json:"device_id"`
	Password  string `json:"password"`
	PublicKey string `json:"public_key"`
	Address   string `json:"address"`
}

// 투표자 수 최신화 관련 함수 users.go
type UserCountPayload struct {
	Count int `json:"count"`
}

// handler.go
type MappingRequest struct {
	DeviceID string `json:"device_id"`
	SenderID string `json:"sender_id"`
}

// handler.go
type MappingResponse struct {
	DeviceID string `json:"device_id"`
	Address  string `json:"address"`
	SenderID string `json:"sender_id"`
}

// kafka.go
type Plant struct {
	ID        int     // 고유 ID
	PlantName string  // 발전소 이름
	Region    string  // 시/도
	City      string  // 시/군/구
	Town      string  // 읍/면/동
	Latitude  float64 // 위도
	Longitude float64 // 경도
}

// kafka.go
// full node로부터 받는 정보
type Location struct {
	Latitude   float64 `json:"latitude"`
	Longitutde float64 `json:"longitude"`
}

type LocationPayload struct {
	Hash     string   `json:"hash"`
	Location Location `json:"location"`
	SenderID string   `json:"sender_id"`
}

// full node로 보내는 결과값
type LocationOutputMessage struct {
	Hash     string  `json:"hash"`
	Output   float64 `json:"output"`
	SenderID string  `json:"sender_id"`
}

type AccountRequest struct {
	NodeID  string `json:"node_id"`
	Address string `json:"user_address"`
}

// 풀노드로 부터 보상을 요청 받는 메세지
type VMemberRequestMessage struct {
	FullnodeID string   `json:"fullnode_id"` // 요청 보낸 풀노드 ID
	Validators []string `json:"validators"`
	Timestamp  string   `json:"timestamp"`
}

// 풀노드에게 보상금을 전송하는 메세지
type MemberRewardOutputMessage struct {
	SenderID string             `json:"sender_id"` // 메시지 송신자 ID
	Rewards  map[string]float64 `json:"rewards"`   // 주소 -> 보상금
}

type TxHashRequest struct {
	Address string `json:"user_address"`
}

type SignatureEvent struct {
	SigID   string  `json:"sig_id"`  // 서명 유일키(해시 등)
	Address string  `json:"address"` // 지갑 주소
	TurnID  string  `json:"turn_id"` // 턴 식별자(예: REC 타임슬롯)
	Delta   float64 `json:"delta"`   // R0 + 투표가중치 (이번 서명의 가산점)
}
