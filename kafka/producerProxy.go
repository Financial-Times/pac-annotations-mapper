package kafka

import (
	"time"

	ftkafka "github.com/Financial-Times/kafka-client-go/kafka"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"errors"
)

const errProducerNotConnected = "producer is not connected to Kafka"

type ProxyProducer struct {
	brokers string
	topic string
	config *sarama.Config
	producer ftkafka.Producer
}

func NewProxyProducer(brokers string, topic string, config *sarama.Config) *ProxyProducer {
	return &ProxyProducer{brokers, topic, config, nil}
}

func (p *ProxyProducer) Connect() {
	connectorLog := log.WithField("brokers", p.brokers).WithField("topic", p.topic)
	for {
		producer, err := ftkafka.NewProducer(p.brokers, p.topic, p.config)
		if err == nil {
			connectorLog.Info("connected to Kafka producer")
			p.producer = producer
			break
		}

		connectorLog.WithError(err).Warn(errProducerNotConnected)
		time.Sleep(time.Minute)
	}
}

func (p *ProxyProducer) SendMessage(message ftkafka.FTMessage) error {
	if p.producer == nil {
		return errors.New(errProducerNotConnected)
	}

	return p.producer.SendMessage(message)
}
