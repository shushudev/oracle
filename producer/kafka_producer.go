package producer

import (
	"oracle/config"

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
	return &kafka.Writer{
		Addr:     kafka.TCP(config.KafkaBrokers...),
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
