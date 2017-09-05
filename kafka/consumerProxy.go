package kafka

import (
	"errors"
	"time"

	ftkafka "github.com/Financial-Times/kafka-client-go/kafka"
	log "github.com/sirupsen/logrus"
	"github.com/wvanbergen/kafka/consumergroup"
)

const errConsumerNotConnected = "consumer is not connected to Kafka"

type ProxyConsumer struct {
	zookeeperConnectionString string
	consumerGroup string
	topics []string
	config *consumergroup.Config
	consumer ftkafka.Consumer
}

func NewProxyConsumer(zookeeperConnectionString string, consumerGroup string, topics []string, config *consumergroup.Config) *ProxyConsumer {
	return &ProxyConsumer{zookeeperConnectionString, consumerGroup, topics, config , nil}
}

func (c *ProxyConsumer) Connect() {
	connectorLog := log.WithField("zookeeper", c.zookeeperConnectionString).WithField("topics", c.topics).WithField("consumerGroup", c.consumerGroup)
	for {
		consumer, err := ftkafka.NewConsumer(c.zookeeperConnectionString, c.consumerGroup, c.topics, c.config)
		if err == nil {
			connectorLog.Info("connected to Kafka consumer")
			c.consumer = consumer
			break
		}

		connectorLog.WithError(err).Warn(errConsumerNotConnected)
		time.Sleep(time.Minute)
	}
}

func (c *ProxyConsumer) StartListening(messageHandler func(message ftkafka.FTMessage) error) {
	if c.consumer == nil {
		c.Connect()
	}

	c.consumer.StartListening(messageHandler)
}

func (c *ProxyConsumer) Shutdown() {
	if c.consumer != nil {
		c.consumer.Shutdown()
	}
}

func (c *ProxyConsumer) ConnectivityCheck() error {
	if c.consumer == nil {
		return errors.New(errConsumerNotConnected)
	}

	// the Kafka consumer healthcheck is not reliable, so see if we can establish a new, independent connection
	healthcheckConsumer, err := ftkafka.NewConsumer(c.zookeeperConnectionString, c.consumerGroup + "-healthcheck", c.topics, c.config)
	if err != nil {
		return err
	}
	defer healthcheckConsumer.Shutdown()

	return nil
}
