package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/Financial-Times/kafka-client-go/kafka"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const (
	testSystemId = "http://cmdb.ft.com/systems/test-system"
	testTxId     = "tid_testing"

	hasBrand = "http://www.ft.com/ontology/classification/isClassifiedBy"
	mentions = "http://www.ft.com/ontology/annotation/mentions"
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

func TestMessageMapped(t *testing.T) {
	whitelist := regexp.MustCompile(strings.Replace(testSystemId, ".", `\.`, -1))
	mp := &mockMessageProducer{}
	mp.On("SendMessage", mock.AnythingOfType("kafka.FTMessage")).Return(nil)

	service := NewAnnotationMapperService(whitelist, mp)

	contentUuid := uuid.NewV4().String()
	brandUuid := uuid.NewV4().String()
	mentionsUuid := uuid.NewV4().String()
	inbound := kafka.FTMessage{
		Headers: map[string]string{
			"Origin-System-Id": testSystemId,
			"X-Request-Id":     testTxId,
		},
		Body: fmt.Sprintf(`{
		"uuid":"%s",
		"submittedBy":"test-user",
		"annotations":[
		    {
		        "predicate":"%s",
		        "id":"%s"
		    },
		    {
		        "predicate":"%s",
		        "id":"%s"
		    }
		]
		}`, contentUuid,
			hasBrand, brandUuid,
			mentions, mentionsUuid),
	}

	err := service.HandleMessage(inbound)
	assert.NoError(t, err)

	mp.AssertExpectations(t)

	require.Len(t, mp.received, 1, "messages sent to producer")

	actual := mp.received[0]
	assert.Equal(t, testTxId, actual.Headers["X-Request-Id"], "transaction_id should be propagated")

	actualBody := ConceptAnnotations{}
	json.NewDecoder(strings.NewReader(actual.Body)).Decode(&actualBody)

	assert.Equal(t, contentUuid, actualBody.UUID, "content uuid")

	actualAnnotations := actualBody.Annotations
	assert.Len(t, actualAnnotations, 2, "annotations")

	foundBrand := false
	foundMentions := false
	for _, ann := range actualAnnotations {
		switch ann.Thing.Predicate {
		case "isClassifiedBy":
			assert.Equal(t, brandUuid, ann.Thing.ID, "brand annotation")
			foundBrand = true

		case "mentions":
			assert.Equal(t, mentionsUuid, ann.Thing.ID, "mentions annotation")
			foundMentions = true
		}
	}
	assert.True(t, foundBrand, "expected brand predicate was not found")
	assert.True(t, foundMentions, "expected mentions predicate was not found")
}

func TestPredicateValidation(t *testing.T) {
	whitelist := regexp.MustCompile(strings.Replace(testSystemId, ".", `\.`, -1))
	mp := &mockMessageProducer{}
	mp.On("SendMessage", mock.AnythingOfType("kafka.FTMessage")).Return(nil)

	service := NewAnnotationMapperService(whitelist, mp)

	contentUuid := uuid.NewV4().String()
	brandUuid := uuid.NewV4().String()
	mentionsUuid := uuid.NewV4().String()
	inbound := kafka.FTMessage{
		Headers: map[string]string{
			"Origin-System-Id": testSystemId,
			"X-Request-Id":     testTxId,
		},
		Body: fmt.Sprintf(`{
		"uuid":"%s",
		"submittedBy":"test-user",
		"annotations":[
		    {
		        "predicate":"%s",
		        "id":"%s"
		    },
		    {
		        "predicate":"%s",
		        "id":"%s"
		    }
		]
		}`, contentUuid,
			hasBrand, brandUuid,
			"http:///www.example.com/annotation/mentions", mentionsUuid),
	}

	err := service.HandleMessage(inbound)
	assert.NoError(t, err)

	mp.AssertExpectations(t)

	require.Len(t, mp.received, 1, "messages sent to producer")

	actual := mp.received[0]
	assert.Equal(t, testTxId, actual.Headers["X-Request-Id"], "transaction_id should be propagated")

	actualBody := ConceptAnnotations{}
	json.NewDecoder(strings.NewReader(actual.Body)).Decode(&actualBody)

	assert.Equal(t, contentUuid, actualBody.UUID, "content uuid")

	actualAnnotations := actualBody.Annotations
	assert.Len(t, actualAnnotations, 1, "annotations")
	assert.Equal(t, brandUuid, actualAnnotations[0].Thing.ID, "brand annotation")
}

func TestSourceNonMatchingWhitelistIsIgnored(t *testing.T) {
	whitelist := regexp.MustCompile(`"http://www\.example\.com/ft-system`)
	mp := &mockMessageProducer{}
	service := NewAnnotationMapperService(whitelist, mp)
	inbound := kafka.FTMessage{
		Headers: map[string]string{"Origin-System-Id": testSystemId},
		Body:    `{"foo":"bar"}`,
	}

	actual := service.HandleMessage(inbound)
	assert.NoError(t, actual)
	mp.AssertExpectations(t)
}

func TestSyntacticallyInvalidJsonIsRejected(t *testing.T) {
	whitelist := regexp.MustCompile(strings.Replace(testSystemId, ".", `\.`, -1))
	mp := &mockMessageProducer{}
	service := NewAnnotationMapperService(whitelist, mp)
	inbound := kafka.FTMessage{
		Headers: map[string]string{"Origin-System-Id": testSystemId},
		Body:    `{"foo":"bar"`,
	}

	actual := service.HandleMessage(inbound)
	assert.Error(t, actual)
	mp.AssertExpectations(t)
}

func TestMessageProducerError(t *testing.T) {
	errmsg := "test error"
	whitelist := regexp.MustCompile(strings.Replace(testSystemId, ".", `\.`, -1))
	mp := &mockMessageProducer{}
	mp.On("SendMessage", mock.AnythingOfType("kafka.FTMessage")).Return(errors.New(errmsg))

	service := NewAnnotationMapperService(whitelist, mp)

	contentUuid := uuid.NewV4().String()
	inbound := kafka.FTMessage{
		Headers: map[string]string{
			"Origin-System-Id": testSystemId,
			"X-Request-Id":     testTxId,
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
		}`, contentUuid),
	}

	err := service.HandleMessage(inbound)
	assert.EqualError(t, err, errmsg)

	mp.AssertExpectations(t)
}
