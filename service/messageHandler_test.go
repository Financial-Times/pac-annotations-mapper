package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/Financial-Times/go-logger/v2"

	"github.com/Financial-Times/kafka-client-go/v3"
	"github.com/google/uuid"
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
	err      error
}

func (p *mockMessageProducer) SendMessage(message kafka.FTMessage) error {
	args := p.Called(message)
	p.received = append(p.received, message)
	p.err = args.Error(0)
	return p.err
}

func (p *mockMessageProducer) ConnectivityCheck() error {
	return nil
}

func (p *mockMessageProducer) Close() error {
	return nil
}

func TestMessageMapped(t *testing.T) {
	log := logger.NewUnstructuredLogger()
	whitelist := regexp.MustCompile(strings.Replace(testSystemID, ".", `\.`, -1))

	contentUUID := uuid.NewString()
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
		"hasBrand": {
			PredicateURI:    "http://www.ft.com/ontology/hasBrand",
			PredicateMapped: "hasBrand",
		},
	}

	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			mp := &mockMessageProducer{}
			mp.On("SendMessage", mock.AnythingOfType("kafka.FTMessage")).Return(nil)

			service := NewAnnotationMapperService(whitelist, mp, log)

			annotationID := uuid.NewString()
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
			service.HandleMessage(inbound)
			require.Len(t, mp.received, 1, "messages sent to producer")

			actual := mp.received[0]
			assert.Equal(t, testTxID, actual.Headers["X-Request-Id"], "transaction_id should be propagated")

			actualBody := MappedAnnotations{}
			err := json.NewDecoder(strings.NewReader(actual.Body)).Decode(&actualBody)
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
}

func TestSourceNonMatchingWhitelistIsIgnored(t *testing.T) {
	log := logger.NewUnstructuredLogger()
	whitelist := regexp.MustCompile(`"http://www\.example\.com/ft-system`)
	mp := &mockMessageProducer{}
	service := NewAnnotationMapperService(whitelist, mp, log)
	inbound := kafka.FTMessage{
		Headers: map[string]string{"Origin-System-Id": testSystemID},
		Body:    `{"foo":"bar"}`,
	}

	service.HandleMessage(inbound)
	assert.Empty(t, mp.received)
	mp.AssertExpectations(t)
}

func TestSyntacticallyInvalidJsonIsRejected(t *testing.T) {
	log := logger.NewUnstructuredLogger()
	whitelist := regexp.MustCompile(strings.Replace(testSystemID, ".", `\.`, -1))
	mp := &mockMessageProducer{}
	service := NewAnnotationMapperService(whitelist, mp, log)
	inbound := kafka.FTMessage{
		Headers: map[string]string{"Origin-System-Id": testSystemID},
		Body:    `{"foo":"bar"`,
	}

	service.HandleMessage(inbound)
	assert.Empty(t, mp.received)
	mp.AssertExpectations(t)
}

func TestMessageProducerError(t *testing.T) {
	log := logger.NewUnstructuredLogger()
	errmsg := errors.New("test error")
	whitelist := regexp.MustCompile(strings.Replace(testSystemID, ".", `\.`, -1))
	mp := &mockMessageProducer{}
	mp.On("SendMessage", mock.AnythingOfType("kafka.FTMessage")).Return(errmsg)

	service := NewAnnotationMapperService(whitelist, mp, log)

	contentUUID := uuid.NewString()
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

	service.HandleMessage(inbound)

	mp.AssertExpectations(t)
	assert.Equal(t, errmsg, mp.err)
}

func TestNilWhitelistIsIgnoredWithErrorLog(t *testing.T) {
	log := logger.NewUnstructuredLogger()
	mp := &mockMessageProducer{}
	service := NewAnnotationMapperService(nil, mp, log)
	inbound := kafka.FTMessage{
		Headers: map[string]string{"Origin-System-Id": testSystemID},
		Body:    `{"foo":"bar"}`,
	}

	service.HandleMessage(inbound)
	mp.AssertExpectations(t)

	assert.Empty(t, mp.received)
}
