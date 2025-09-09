package producer

import (
	"fmt"
	"oracle/config"

	"github.com/IBM/sarama"
	"github.com/segmentio/kafka-go"
)

var RewardProducer *kafka.Writer

// 디바이스-주소 매핑 결과를 보내는 Writer
func NewMappingWriter() *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(config.KafkaBrokers...),
		Topic:    config.TopicDeviceIdToAddressProducer,
		Balancer: &kafka.LeastBytes{},
	}
}

func NewVoteMemberWriter() *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(config.KafkaBrokers...),
		Topic:    config.TopicVoteMemberProducer,
		Balancer: &kafka.LeastBytes{},
	}
}

func NewLocationWriter() *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(config.KafkaBrokers...),
		Topic:    config.TopicResultLocationProducer,
		Balancer: &kafka.LeastBytes{},
	}
}

func NewAccounCreatetWriter() *kafka.Writer { // 회원가입
	return &kafka.Writer{
		Addr:     kafka.TCP(config.KafkaBrokers...),
		Topic:    config.TopicCreateAccountProducer,
		Balancer: &kafka.LeastBytes{},
	}
}

func NewTxHashWriter() *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(config.KafkaBrokers...),
		Topic:    config.TopicResultTxhashProducer,
		Balancer: &kafka.LeastBytes{},
	}
}

func NewCollateralsWriter() *kafka.Writer {
	if len(config.KafkaBrokers) == 0 {
		panic("KafkaBrokers is empty!")
	}

	return &kafka.Writer{
		Addr:     kafka.TCP(config.KafkaBrokers...), // host:port 형태 필수
		Topic:    config.TopicCollateralsProducer,
		Balancer: &kafka.LeastBytes{},
	}
}

func NewBurnWriter() *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(config.KafkaBrokers...),
		Topic:    config.TopicBurnProducer,
		Balancer: &kafka.LeastBytes{},
	}
}
func InitRewardProducer() *kafka.Writer {
	RewardProducer = &kafka.Writer{
		Addr:     kafka.TCP(config.KafkaBrokers...),
		Topic:    config.TopicResultVMemberReward,
		Balancer: &kafka.LeastBytes{},
	}
	return RewardProducer
}

func NewSaramaProducer(brokers []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // 모든 ISR ack
	config.Producer.Retry.Max = 5                    // 재시도 횟수
	config.Producer.Return.Successes = true          // 성공시 채널 반환

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("sarama producer 생성 실패: %v", err)
	}
	return producer, nil
}
