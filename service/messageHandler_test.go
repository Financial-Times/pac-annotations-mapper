package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"

	logTest "github.com/Financial-Times/go-logger/test"
	"github.com/Financial-Times/kafka-client-go/kafka"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const (
	testSystemID = "http://cmdb.ft.com/systems/test-system"
	testTxID     = "tid_testing"
)

type mockMessageProducer struct {
	mock.Mock
	received []kafka.FTMessage
}

func (p *mockMessageProducer) SendMessage(message kafka.FTMessage) error {
	args := p.Called(message)
	p.received = append(p.received, message)
	return args.Error(0)
}

func (p *mockMessageProducer) ConnectivityCheck() error {
	return nil
}

func (p *mockMessageProducer) Shutdown() {

}

func TestMessageMappedNew(t *testing.T) {
	hook := logTest.NewTestHook("test")
	whitelist := regexp.MustCompile(strings.Replace(testSystemID, ".", `\.`, -1))

	contentUUID := uuid.NewV4().String()
	tests := map[string]struct {
		PredicateURI    string
		PredicateMapped string
		ShouldSkip      bool
	}{
		"isClassifiedBy": {
			PredicateURI:    "http://www.ft.com/ontology/classification/isClassifiedBy",
			PredicateMapped: "isClassifiedBy",
		},
		"mentions": {
			PredicateURI:    "http://www.ft.com/ontology/annotation/mentions",
			PredicateMapped: "mentions",
		},
		"implicitlyClassifiedBy": {
			PredicateURI:    "http://www.ft.com/ontology/implicitlyClassifiedBy",
			PredicateMapped: "implicitlyClassifiedBy",
		},
		"hasAuthor": {
			PredicateURI:    "http://www.ft.com/ontology/annotation/hasAuthor",
			PredicateMapped: "hasAuthor",
		},
		"hasContributor": {
			PredicateURI:    "http://www.ft.com/ontology/hasContributor",
			PredicateMapped: "hasContributor",
		},
		"about": {
			PredicateURI:    "http://www.ft.com/ontology/annotation/about",
			PredicateMapped: "about",
		},
		"hasDisplayTag": {
			PredicateURI:    "http://www.ft.com/ontology/hasDisplayTag",
			PredicateMapped: "hasDisplayTag",
		},
		"invalid-predicate": {
			PredicateURI: "invalid predicate",
			ShouldSkip:   true,
		},
	}

	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			mp := &mockMessageProducer{}
			mp.On("SendMessage", mock.AnythingOfType("kafka.FTMessage")).Return(nil)

			service := NewAnnotationMapperService(whitelist, mp)

			annotationID := uuid.NewV4().String()
			inbound := kafka.FTMessage{
				Headers: map[string]string{
					"Origin-System-Id": testSystemID,
					"X-Request-Id":     testTxID,
				},
				Body: fmt.Sprintf(`{
				"uuid":"%s",
				"submittedBy":"test-user",
				"annotations":[
					{
						"predicate":"%s",
						"id":"%s"
					}
				]
				}`, contentUUID, test.PredicateURI, annotationID),
			}
			err := service.HandleMessage(inbound)
			assert.NoError(t, err)
			mp.AssertExpectations(t)
			require.Len(t, mp.received, 1, "messages sent to producer")

			actual := mp.received[0]
			assert.Equal(t, testTxID, actual.Headers["X-Request-Id"], "transaction_id should be propagated")

			actualBody := MappedAnnotations{}
			err = json.NewDecoder(strings.NewReader(actual.Body)).Decode(&actualBody)
			assert.NoError(t, err)

			assert.Equal(t, contentUUID, actualBody.UUID, "content uuid")

			actualAnnotations := actualBody.Annotations
			if test.ShouldSkip {
				if len(actualAnnotations) != 0 {
					t.Fatal("did not expect to map annotation")
				}
				return
			}

			if len(actualAnnotations) != 1 {
				t.Fatal("expected one annotation to be mapped")
			}

			annotation := actualAnnotations[0]
			assert.Equal(t, test.PredicateMapped, annotation.Concept.Predicate)
			assert.Equal(t, annotationID, annotation.Concept.ID)

		})
	}
	logTest.Assert(t, hook.LastEntry()).
		HasMonitoringEvent("Map", testTxID, "Annotations").
		HasValidFlag(true).
		HasUUID(contentUUID)

}

func TestSourceNonMatchingWhitelistIsIgnored(t *testing.T) {
	logTest.NewTestHook("test")
	whitelist := regexp.MustCompile(`"http://www\.example\.com/ft-system`)
	mp := &mockMessageProducer{}
	service := NewAnnotationMapperService(whitelist, mp)
	inbound := kafka.FTMessage{
		Headers: map[string]string{"Origin-System-Id": testSystemID},
		Body:    `{"foo":"bar"}`,
	}

	actual := service.HandleMessage(inbound)
	assert.NoError(t, actual)
	mp.AssertExpectations(t)
}

func TestSyntacticallyInvalidJsonIsRejected(t *testing.T) {
	hook := logTest.NewTestHook("test")
	whitelist := regexp.MustCompile(strings.Replace(testSystemID, ".", `\.`, -1))
	mp := &mockMessageProducer{}
	service := NewAnnotationMapperService(whitelist, mp)
	inbound := kafka.FTMessage{
		Headers: map[string]string{"Origin-System-Id": testSystemID},
		Body:    `{"foo":"bar"`,
	}

	actual := service.HandleMessage(inbound)
	assert.Error(t, actual)
	mp.AssertExpectations(t)

	// check of monitoring logging
	logTest.Assert(t, hook.LastEntry()).
		HasMonitoringEvent("Map", "unknown", "Annotations").
		HasValidFlag(false)
}

func TestMessageProducerError(t *testing.T) {
	hook := logTest.NewTestHook("test")
	errmsg := "test error"
	whitelist := regexp.MustCompile(strings.Replace(testSystemID, ".", `\.`, -1))
	mp := &mockMessageProducer{}
	mp.On("SendMessage", mock.AnythingOfType("kafka.FTMessage")).Return(errors.New(errmsg))

	service := NewAnnotationMapperService(whitelist, mp)

	contentUUID := uuid.NewV4().String()
	inbound := kafka.FTMessage{
		Headers: map[string]string{
			"Origin-System-Id": testSystemID,
			"X-Request-Id":     testTxID,
		},
		Body: fmt.Sprintf(`{
		"uuid":"%s",
		"submittedBy":"test-user",
		"annotations":[
		    {
		        "predicate":"foo",
		        "id":"bar"
		    }
		]
		}`, contentUUID),
	}

	err := service.HandleMessage(inbound)
	assert.EqualError(t, err, errmsg)

	mp.AssertExpectations(t)

	// check of monitoring logging
	logTest.Assert(t, hook.LastEntry()).
		HasMonitoringEvent("Map", testTxID, "Annotations").
		HasValidFlag(true).
		HasUUID(contentUUID)
}

func TestNilWhitelistIsIgnoredWithErrorLog(t *testing.T) {
	hook := logTest.NewTestHook("test")
	mp := &mockMessageProducer{}
	service := NewAnnotationMapperService(nil, mp)
	inbound := kafka.FTMessage{
		Headers: map[string]string{"Origin-System-Id": testSystemID},
		Body:    `{"foo":"bar"}`,
	}

	actual := service.HandleMessage(inbound)
	assert.NoError(t, actual)
	mp.AssertExpectations(t)

	actualLog := hook.LastEntry()
	assert.Equal(t, "Skipping this message because the whitelist is invalid.", actualLog.Message, "log message")
	assert.Equal(t, "error", actualLog.Level.String(), "log level")
}
