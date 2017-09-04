package health

import (
	"net/http"

	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/kafka-client-go/kafka"
	"github.com/Financial-Times/service-status-go/gtg"
)

const HealthPath = "/__health"

type HealthCheck struct {
	appSystemCode string
	appName       string
	appDescription string
	consumer kafka.Consumer
}

func NewHealthCheck(appSystemCode string, appName string, appDescription string, c kafka.Consumer) *HealthCheck {
	return &HealthCheck{
		appSystemCode: appSystemCode,
		appName: appName,
		appDescription: appDescription,
		consumer: c,
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
	return []fthealth.Check{h.readQueueCheck()}
}

func (h *HealthCheck) readQueueCheck() fthealth.Check {
	return fthealth.Check{
		ID:               "read-message-queue-reachable",
		Name:             "Read Message Queue Reachable",
		Severity:         1,
		BusinessImpact:   "PAC Metadata can't be read from queue. This will negatively impact metadata availability.",
		TechnicalSummary: "Read message queue is not reachable/healthy",
		PanicGuide:       "https://dewey.ft.com/pac-annotations-mapper.html",
		Checker:          h.checkKafkaConnectivity,
	}
}

func (h *HealthCheck) GTG() gtg.Status {
	consumerCheck := func() gtg.Status {
		return gtgCheck(h.checkKafkaConnectivity)
	}
	producerCheck := func() gtg.Status {
		return gtgCheck(h.checkKafkaConnectivity)
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

func (h *HealthCheck) checkKafkaConnectivity() (string, error) {
	if err := h.consumer.ConnectivityCheck(); err != nil {
		return "Error connecting with Kafka", err
	}
	return "Successfully connected to Kafka", nil
}
