package kafka

import (
	"testing"

	ftkafka "github.com/Financial-Times/kafka-client-go/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockConsumer struct {
	mock.Mock
}

func (c *mockConsumer) StartListening(messageHandler func(message ftkafka.FTMessage) error) {
	c.Called(messageHandler)
}

func (c *mockConsumer) Shutdown() {
	c.Called()
}

func (c *mockConsumer) ConnectivityCheck() error {
	return c.Called().Error(0)
}

func TestStartListeningIsForwarded(t *testing.T) {
	mc := mockConsumer{}
	mc.On("StartListening", mock.AnythingOfType("func(kafka.FTMessage) error"))
	c := ProxyConsumer{consumer: &mc}

	c.StartListening(func(msg ftkafka.FTMessage) error { return nil })
	mc.AssertExpectations(t)
}

func TestConsumerCheckWithoutConsumerReturnsNotConnected(t *testing.T) {
	c := ProxyConsumer{}

	actual := c.ConnectivityCheck()
	assert.EqualError(t, actual, errConsumerNotConnected)
}

func TestShutdownIsForwarded(t *testing.T) {
	mc := mockConsumer{}
	mc.On("Shutdown")
	c := ProxyConsumer{consumer: &mc}

	c.Shutdown()
	mc.AssertExpectations(t)
}

func TestShutdownWithoutConsumerIsAllowed(t *testing.T) {
	c := ProxyConsumer{}

	c.Shutdown()
	// nothing to assert, but a panic would cause this test to fail
}
