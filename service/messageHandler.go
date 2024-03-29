package service

import (
	"encoding/json"
	"regexp"
	"time"

	"github.com/Financial-Times/go-logger/v2"

	"github.com/Financial-Times/kafka-client-go/v3"
	"github.com/google/uuid"
)

const messageTimestampDateFormat = "2006-01-02T15:04:05.000Z"

const mapperEvent = "Map"
const annotationsType = "Annotations"

var predicates = map[string]string{
	"http://www.ft.com/ontology/hasBrand":                      "hasBrand",
	"http://www.ft.com/ontology/classification/isClassifiedBy": "isClassifiedBy",
	"http://www.ft.com/ontology/implicitlyClassifiedBy":        "implicitlyClassifiedBy",
	"http://www.ft.com/ontology/annotation/hasAuthor":          "hasAuthor",
	"http://www.ft.com/ontology/hasContributor":                "hasContributor",
	"http://www.ft.com/ontology/annotation/about":              "about",
	"http://www.ft.com/ontology/hasDisplayTag":                 "hasDisplayTag",
	"http://www.ft.com/ontology/annotation/mentions":           "mentions",
}

type kafkaProducer interface {
	SendMessage(message kafka.FTMessage) error
}

type AnnotationMapperService struct {
	whitelist       *regexp.Regexp
	messageProducer kafkaProducer
	log             *logger.UPPLogger
}

func NewAnnotationMapperService(whitelist *regexp.Regexp, messageProducer kafkaProducer, log *logger.UPPLogger) *AnnotationMapperService {
	return &AnnotationMapperService{whitelist, messageProducer, log}
}

func (mapper *AnnotationMapperService) HandleMessage(msg kafka.FTMessage) {
	tid, found := msg.Headers["X-Request-Id"]
	if !found {
		tid = "unknown"
	}

	requestLog := mapper.log.WithTransactionID(tid)
	if mapper.whitelist == nil {
		requestLog.Error("Skipping this message because the whitelist is invalid.")
		return
	}

	systemCode := msg.Headers["Origin-System-Id"]
	if !mapper.whitelist.MatchString(systemCode) {
		requestLog.Infof("Skipping annotations published with Origin-System-Id \"%v\". It does not match the configured whitelist.", systemCode)
		return
	}

	var metadataPublishEvent PacMetadataPublishEvent
	err := json.Unmarshal([]byte(msg.Body), &metadataPublishEvent)
	if err != nil {
		mapper.log.WithMonitoringEvent(mapperEvent, tid, annotationsType).
			WithValidFlag(false).
			WithError(err).
			Error("Cannot unmarshal message body")
		return
	}

	requestLog = requestLog.WithUUID(metadataPublishEvent.UUID)
	requestLog.Info("Processing metadata publish event")

	annotations := []annotation{}
	for _, value := range metadataPublishEvent.Annotations {
		ann := mapper.buildAnnotation(value)
		if ann != nil {
			annotations = append(annotations, *ann)
		} else {
			requestLog.WithField("metadata", value).Warn("metadata for an unsupported predicate was not mapped")
		}
	}

	mappedAnnotations := MappedAnnotations{UUID: metadataPublishEvent.UUID, Annotations: annotations}

	marshalledAnnotations, err := json.Marshal(mappedAnnotations)
	if err != nil {
		mapper.log.WithMonitoringEvent(mapperEvent, tid, annotationsType).
			WithUUID(metadataPublishEvent.UUID).
			WithValidFlag(true).
			WithError(err).
			Error("Error marshalling the concept annotations")
		return
	}

	var headers = buildMappedAnnotationsHeader(msg.Headers)
	message := kafka.FTMessage{Headers: headers, Body: string(marshalledAnnotations)}
	err = mapper.messageProducer.SendMessage(message)
	if err != nil {
		mapper.log.WithMonitoringEvent(mapperEvent, tid, annotationsType).
			WithUUID(metadataPublishEvent.UUID).
			WithValidFlag(true).
			WithError(err).
			Error("Error sending concept annotations to queue")
		return
	}

	mapper.log.WithMonitoringEvent(mapperEvent, tid, annotationsType).
		WithUUID(metadataPublishEvent.UUID).
		WithValidFlag(true).
		Info("Sent annotation message to queue")
}

func (mapper *AnnotationMapperService) buildAnnotation(metadata PacMetadataAnnotation) *annotation {
	var ann *annotation

	if predicate, found := predicates[metadata.Predicate]; found {
		ann = &annotation{
			Concept: concept{
				ID:        metadata.ConceptId,
				Predicate: predicate,
			},
		}
	}

	return ann
}

func buildMappedAnnotationsHeader(publishEventHeaders map[string]string) map[string]string {
	return map[string]string{
		"Message-Id":        uuid.NewString(),
		"Message-Type":      "concept-annotation",
		"Content-Type":      publishEventHeaders["Content-Type"],
		"X-Request-Id":      publishEventHeaders["X-Request-Id"],
		"Origin-System-Id":  publishEventHeaders["Origin-System-Id"],
		"Message-Timestamp": time.Now().Format(messageTimestampDateFormat),
	}
}
