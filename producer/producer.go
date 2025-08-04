package producer

import (
	"oracle/config"

	"github.com/segmentio/kafka-go"
)

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
