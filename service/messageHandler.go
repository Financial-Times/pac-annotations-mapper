package service

import (
	"encoding/json"
	"regexp"
	"strings"
	"time"

	"github.com/Financial-Times/kafka-client-go/kafka"
	"github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
)

const messageTimestampDateFormat = "2006-01-02T15:04:05.000Z"

type AnnotationMapperService struct {
	whitelist       *regexp.Regexp
	messageProducer kafka.Producer
}

func NewAnnotationMapperService(whitelist *regexp.Regexp, messageProducer kafka.Producer) *AnnotationMapperService {
	return &AnnotationMapperService{whitelist, messageProducer}
}

func (mapper *AnnotationMapperService) HandleMessage(msg kafka.FTMessage) error {
	tid := msg.Headers["X-Request-Id"]
	requestLog := log.WithField("transaction_id", tid)

	systemCode := msg.Headers["Origin-System-Id"]
	if !mapper.whitelist.MatchString(systemCode) {
		requestLog.Infof("Skipping annotations published with Origin-System-Id \"%v\". It does not match the configured whitelist.", systemCode)
		return nil
	}

	var metadataPublishEvent PacMetadataPublishEvent
	err := json.Unmarshal([]byte(msg.Body), &metadataPublishEvent)
	if err != nil {
		requestLog.Error("Cannot unmarshal message body", err)
		return err
	}

	requestLog = requestLog.WithField("uuid", metadataPublishEvent.UUID)
	requestLog.Info("Processing metadata publish event")

	annotations := []annotation{}
	for _, value := range metadataPublishEvent.Annotations {
		annotations = append(annotations, mapper.buildAnnotation(value))
	}

	conceptAnnotations := ConceptAnnotations{UUID: metadataPublishEvent.UUID, Annotations: annotations}

	marshalledAnnotations, err := json.Marshal(conceptAnnotations)
	if err != nil {
		requestLog.Error("Error marshalling the concept annotations", err)
		return err
	}

	var headers = buildConceptAnnotationsHeader(msg.Headers)
	message := kafka.FTMessage{Headers: headers, Body: string(marshalledAnnotations)}
	err = mapper.messageProducer.SendMessage(message)
	if err != nil {
		requestLog.Error("Error sending concept annotation to queue", err)
		return err
	}
	requestLog.Info("Sent annotation message to queue")
	return nil
}

func (mapper *AnnotationMapperService) buildAnnotation(metadata PacMetadataAnnotation) annotation {
	predicate := metadata.Predicate[strings.LastIndex(metadata.Predicate, "/")+1:]
	thing := thing{ID: metadata.ConceptId, Predicate: predicate}
	ann := annotation{Thing: thing}

	return ann
}

func buildConceptAnnotationsHeader(publishEventHeaders map[string]string) map[string]string {
	return map[string]string{
		"Message-Id":        uuid.NewV4().String(),
		"Message-Type":      "concept-annotation",
		"Content-Type":      publishEventHeaders["Content-Type"],
		"X-Request-Id":      publishEventHeaders["X-Request-Id"],
		"Origin-System-Id":  publishEventHeaders["Origin-System-Id"],
		"Message-Timestamp": time.Now().Format(messageTimestampDateFormat),
	}
}
