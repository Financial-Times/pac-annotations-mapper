package health

import (
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/Financial-Times/kafka-client-go/kafka"
	"github.com/stretchr/testify/assert"
)

type mockConsumer struct {
	err error
}

type mockProducer struct {
	connectivityErr error
}

func (mc mockConsumer) ConnectivityCheck() error {
	return mc.err
}

func (mc mockConsumer) StartListening(messageHandler func(message kafka.FTMessage) error) {
	return
}

func (mc mockConsumer) Shutdown() {
	return
}

func (mp mockProducer) SendMessage(message kafka.FTMessage) error {
	return nil
}

func (mp mockProducer) ConnectivityCheck() error {
	return mp.connectivityErr
}

func TestHappyHealthCheck(t *testing.T) {
	hc := NewHealthCheck("test-system-code", "test-app-name", "test-app-desc", nil, mockConsumer{nil}, mockProducer{})

	req := httptest.NewRequest("GET", "http://example.com/__health", nil)
	w := httptest.NewRecorder()

	hc.Health()(w, req)

	assert.Equal(t, 200, w.Code, "It should return HTTP 200 OK")
	assert.Contains(t, w.Body.String(), `"name":"Read Message Queue Reachable","ok":true`, "Healthcheck should be happy")
}

func TestHealthCheckWithUnhappyConsumer(t *testing.T) {
	hc := HealthCheck{"test-system-code", "test-app-name", "test-app-desc", nil, mockConsumer{errors.New("Error connecting to the queue")}, mockProducer{}}

	req := httptest.NewRequest("GET", "http://example.com/__health", nil)
	w := httptest.NewRecorder()

	hc.Health()(w, req)

	assert.Equal(t, 200, w.Code, "It should return HTTP 200 OK")
	assert.Contains(t, w.Body.String(), `"name":"Read Message Queue Reachable","ok":false`, "Read message queue healthcheck should be unhappy")
}

func TestHealthCheckWithUnhappyProducer(t *testing.T) {
	hc := HealthCheck{"test-system-code", "test-app-name", "test-app-desc", nil, mockConsumer{nil}, mockProducer{errors.New("Error connecting to the queue")}}

	req := httptest.NewRequest("GET", "http://example.com/__health", nil)
	w := httptest.NewRecorder()

	hc.Health()(w, req)

	assert.Equal(t, 200, w.Code, "It should return HTTP 200 OK")
	assert.Contains(t, w.Body.String(), `"name":"Write Message Queue Reachable","ok":false`, "Write message queue healthcheck should be unhappy")
}

func TestHealthCheckWithWhitelistError(t *testing.T) {
	whitelistErr := errors.New("test error")
	hc := NewHealthCheck("test-system-code", "test-app-name", "test-app-desc", whitelistErr, mockConsumer{nil}, mockProducer{})

	req := httptest.NewRequest("GET", "http://example.com/__health", nil)
	w := httptest.NewRecorder()

	hc.Health()(w, req)

	assert.Equal(t, 200, w.Code, "It should return HTTP 200 OK")
	assert.Contains(t, w.Body.String(), `"name":"Message Whitelist Filter","ok":false`, "Whitelist healthcheck should be unhappy")
}

func TestGTGHappyFlow(t *testing.T) {
	hc := HealthCheck{"test-system-code", "test-app-name", "test-app-desc", nil, mockConsumer{nil}, mockProducer{}}

	status := hc.GTG()
	assert.True(t, status.GoodToGo)
	assert.Empty(t, status.Message)
}

func TestGTGBrokenConsumer(t *testing.T) {
	hc := HealthCheck{"test-system-code", "test-app-name", "test-app-desc", nil, mockConsumer{errors.New("Error connecting to the queue")}, mockProducer{}}

	status := hc.GTG()
	assert.False(t, status.GoodToGo)
	assert.Equal(t, "Error connecting to the queue", status.Message)
}

func TestGTGBrokenProducer(t *testing.T) {
	hc := HealthCheck{"test-system-code", "test-app-name", "test-app-desc", nil, mockConsumer{}, mockProducer{errors.New("Error connecting to the queue")}}

	status := hc.GTG()
	assert.False(t, status.GoodToGo)
	assert.Equal(t, "Error connecting to the queue", status.Message)
}
