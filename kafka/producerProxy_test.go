package kafka

import (
	"testing"

	ftkafka "github.com/Financial-Times/kafka-client-go/kafka"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/assert"
)

type mockProducer struct {
	mock.Mock
}

func (p *mockProducer) SendMessage(message ftkafka.FTMessage) error {
	args := p.Called(message)
	return args.Error(0)
}

func TestProxyProducerForwardsToProducer(t *testing.T) {
	mp := mockProducer{}
	mp.On("SendMessage", mock.AnythingOfType("kafka.FTMessage")).Return(nil)
	p := ProxyProducer{producer: &mp}

	msg := ftkafka.FTMessage{
		Headers: map[string]string{
			"X-Request-Id":     "test",
		},
		Body: `{"foo":"bar"}`,
	}

	actual := p.SendMessage(msg)
	assert.NoError(t, actual)
	mp.AssertExpectations(t)
}

func TestNoProducerReturnsNotConnected(t *testing.T) {
	p := ProxyProducer{}

	msg := ftkafka.FTMessage{
		Headers: map[string]string{
			"X-Request-Id":     "test",
		},
		Body: `{"foo":"bar"}`,
	}

	actual := p.SendMessage(msg)
	assert.EqualError(t, actual, errProducerNotConnected)
}
