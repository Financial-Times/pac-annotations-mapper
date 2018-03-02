package health

import (
	"net/http"

	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/kafka-client-go/kafka"
	"github.com/Financial-Times/service-status-go/gtg"
)

const HealthPath = "/__health"

type HealthCheck struct {
	appSystemCode  string
	appName        string
	appDescription string
	whitelistError error
	consumer       kafka.Consumer
	producer       kafka.Producer
}

func NewHealthCheck(appSystemCode string, appName string, appDescription string, whitelistErr error, c kafka.Consumer, p kafka.Producer) *HealthCheck {
	return &HealthCheck{
		appSystemCode:  appSystemCode,
		appName:        appName,
		appDescription: appDescription,
		whitelistError: whitelistErr,
		consumer:       c,
		producer:       p,
	}
}

func (h *HealthCheck) Health() func(w http.ResponseWriter, r *http.Request) {
	hc := fthealth.HealthCheck{
		SystemCode:  h.appSystemCode,
		Name:        h.appName,
		Description: h.appDescription,
		Checks:      h.Checks(),
	}
	return fthealth.Handler(hc)
}

func (h *HealthCheck) Checks() []fthealth.Check {
	checks := []fthealth.Check{}
	if h.whitelistError != nil {
		checks = append(checks, h.whitelistCheck())
	}
	checks = append(checks, h.readQueueCheck(), h.writeQueueCheck())
	return checks
}

func (h *HealthCheck) whitelistCheck() fthealth.Check {
	return fthealth.Check{
		ID:               "message-whitelist",
		Name:             "Message Whitelist Filter",
		Severity:         2,
		BusinessImpact:   "No metadata will be mapped to UPP. This will negatively impact metadata availability.",
		TechnicalSummary: "The whitelist configuration for this mapper is invalid",
		PanicGuide:       "https://dewey.ft.com/pac-annotations-mapper.html",
		Checker: func() (string, error) {
			return "Whitelist regex is invalid", h.whitelistError
		},
	}
}

func (h *HealthCheck) readQueueCheck() fthealth.Check {
	return fthealth.Check{
		ID:               "read-message-queue-reachable",
		Name:             "Read Message Queue Reachable",
		Severity:         2,
		BusinessImpact:   "PAC Metadata can't be read from queue. This will negatively impact metadata availability.",
		TechnicalSummary: "Read message queue is not reachable/healthy",
		PanicGuide:       "https://dewey.ft.com/pac-annotations-mapper.html",
		Checker:          h.checkKafkaConsumerConnectivity,
	}
}

func (h *HealthCheck) writeQueueCheck() fthealth.Check {
	return fthealth.Check{
		ID:               "write-message-queue-reachable",
		Name:             "Write Message Queue Reachable",
		Severity:         2,
		BusinessImpact:   "Mapped Metadata can't be written to queue. This will negatively impact metadata availability.",
		TechnicalSummary: "Write message queue is not reachable/healthy",
		PanicGuide:       "https://dewey.ft.com/pac-annotations-mapper.html",
		Checker:          h.checkKafkaProducerConnectivity,
	}
}

func (h *HealthCheck) GTG() gtg.Status {
	consumerCheck := func() gtg.Status {
		return gtgCheck(h.checkKafkaConsumerConnectivity)
	}
	producerCheck := func() gtg.Status {
		return gtgCheck(h.checkKafkaProducerConnectivity)
	}

	return gtg.FailFastParallelCheck([]gtg.StatusChecker{
		consumerCheck,
		producerCheck,
	})()
}

func gtgCheck(handler func() (string, error)) gtg.Status {
	if _, err := handler(); err != nil {
		return gtg.Status{GoodToGo: false, Message: err.Error()}
	}
	return gtg.Status{GoodToGo: true}
}

func (h *HealthCheck) checkKafkaConsumerConnectivity() (string, error) {
	if err := h.consumer.ConnectivityCheck(); err != nil {
		return "Error connecting with Kafka", err
	}
	return "Successfully connected to Kafka", nil
}

func (h *HealthCheck) checkKafkaProducerConnectivity() (string, error) {
	if err := h.producer.ConnectivityCheck(); err != nil {
		return "Error connecting with Kafka", err
	}
	return "Successfully connected to Kafka", nil
}
