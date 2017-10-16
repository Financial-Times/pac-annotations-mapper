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
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const (
	testSystemId = "http://cmdb.ft.com/systems/test-system"
	testTxId     = "tid_testing"

	hasBrand               = "http://www.ft.com/ontology/classification/isClassifiedBy"
	mentions               = "http://www.ft.com/ontology/annotation/mentions"
	implicitlyClassifiedBy = "http://www.ft.com/ontology/implicitlyClassifiedBy"
	hasAuthor              = "http://www.ft.com/ontology/annotation/hasAuthor"
	hasContributor         = "http://www.ft.com/ontology/hasContributor"
	about                  = "http://www.ft.com/ontology/annotation/about"
	hasDisplayTag          = "http://www.ft.com/ontology/hasDisplayTag"
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

func TestMessageMapped(t *testing.T) {
	hook := logTest.NewTestHook("test")
	whitelist := regexp.MustCompile(strings.Replace(testSystemId, ".", `\.`, -1))
	mp := &mockMessageProducer{}
	mp.On("SendMessage", mock.AnythingOfType("kafka.FTMessage")).Return(nil)

	service := NewAnnotationMapperService(whitelist, mp)

	contentUuid := uuid.NewV4().String()
	brandUuid := uuid.NewV4().String()
	mentionsUuid := uuid.NewV4().String()
	implicitlyClassifiedByUuid := uuid.NewV4().String()
	hasAuthorUuid := uuid.NewV4().String()
	hasContributorUuid := uuid.NewV4().String()
	aboutUuid := uuid.NewV4().String()
	hasDisplayTagUuid := uuid.NewV4().String()

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
		    },
		    {
		        "predicate":"%s",
		        "id":"%s"
		    },
		    {
		        "predicate":"%s",
		        "id":"%s"
		    },
		    {
		        "predicate":"%s",
		        "id":"%s"
		    },
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
			implicitlyClassifiedBy, implicitlyClassifiedByUuid,
			hasAuthor, hasAuthorUuid,
			hasContributor, hasContributorUuid,
			about, aboutUuid,
			hasDisplayTag, hasDisplayTagUuid,
			mentions, mentionsUuid),
	}

	err := service.HandleMessage(inbound)
	assert.NoError(t, err)

	mp.AssertExpectations(t)

	require.Len(t, mp.received, 1, "messages sent to producer")

	actual := mp.received[0]
	assert.Equal(t, testTxId, actual.Headers["X-Request-Id"], "transaction_id should be propagated")

	actualBody := MappedAnnotations{}
	json.NewDecoder(strings.NewReader(actual.Body)).Decode(&actualBody)

	assert.Equal(t, contentUuid, actualBody.UUID, "content uuid")

	actualAnnotations := actualBody.Annotations
	assert.Len(t, actualAnnotations, 7, "annotations")

	foundBrand := false
	foundMentions := false
	foundImplicitlyClassifiedBy := false
	foundHasAuthor := false
	foundHasContributor := false
	foundAbout := false
	foundHasDisplayTag := false

	for _, ann := range actualAnnotations {
		switch ann.Concept.Predicate {
		case "isClassifiedBy":
			assert.Equal(t, brandUuid, ann.Concept.ID, "brand annotation")
			foundBrand = true

		case "mentions":
			assert.Equal(t, mentionsUuid, ann.Concept.ID, "mentions annotation")
			foundMentions = true

		case "implicitlyClassifiedBy":
			assert.Equal(t, implicitlyClassifiedByUuid, ann.Concept.ID, "implicitlyClassifiedBy annotation")
			foundImplicitlyClassifiedBy = true

		case "hasAuthor":
			assert.Equal(t, hasAuthorUuid, ann.Concept.ID, "hasAuthor annotation")
			foundHasAuthor = true

		case "hasContributor":
			assert.Equal(t, hasContributorUuid, ann.Concept.ID, "hasContributor annotation")
			foundHasContributor = true

		case "about":
			assert.Equal(t, aboutUuid, ann.Concept.ID, "about annotation")
			foundAbout = true

		case "hasDisplayTag":
			assert.Equal(t, hasDisplayTagUuid, ann.Concept.ID, "hasDisplayTag annotation")
			foundHasDisplayTag = true
		}

	}
	assert.True(t, foundBrand, "expected brand predicate was not found")
	assert.True(t, foundMentions, "expected mentions predicate was not found")
	assert.True(t, foundImplicitlyClassifiedBy, "expected implicitlyClassifiedBy predicate was not found")
	assert.True(t, foundHasAuthor, "expected hasAuthor predicate was not found")
	assert.True(t, foundHasContributor, "expected hasContributor predicate was not found")
	assert.True(t, foundAbout, "expected about predicate was not found")
	assert.True(t, foundHasDisplayTag, "expected hasDisplayTag predicate was not found")

	// check of monitoring logging
	logTest.Assert(t, hook.LastEntry()).
		HasMonitoringEvent("Map", testTxId, "annotations").
		HasValidFlag(true)
}

func TestPredicateValidation(t *testing.T) {
	hook := logTest.NewTestHook("test")
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

	actualBody := MappedAnnotations{}
	json.NewDecoder(strings.NewReader(actual.Body)).Decode(&actualBody)

	assert.Equal(t, contentUuid, actualBody.UUID, "content uuid")

	actualAnnotations := actualBody.Annotations
	assert.Len(t, actualAnnotations, 1, "annotations")
	assert.Equal(t, brandUuid, actualAnnotations[0].Concept.ID, "brand annotation")

	// check of monitoring logging
	logTest.Assert(t, hook.LastEntry()).
		HasMonitoringEvent("Map", testTxId, "annotations").
		HasValidFlag(true)
}

func TestSourceNonMatchingWhitelistIsIgnored(t *testing.T) {
	logTest.NewTestHook("test")
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
	hook := logTest.NewTestHook("test")
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

	// check of monitoring logging
	logTest.Assert(t, hook.LastEntry()).
		HasMonitoringEvent("Map", "unknown", "annotations").
		HasValidFlag(false)
}

func TestMessageProducerError(t *testing.T) {
	hook := logTest.NewTestHook("test")
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

	// check of monitoring logging
	logTest.Assert(t, hook.LastEntry()).
		HasMonitoringEvent("Map", testTxId, "annotations").
		HasValidFlag(true)
}

func TestNilWhitelistIsIgnoredWithErrorLog(t *testing.T) {
	hook := logTest.NewTestHook("test")
	mp := &mockMessageProducer{}
	service := NewAnnotationMapperService(nil, mp)
	inbound := kafka.FTMessage{
		Headers: map[string]string{"Origin-System-Id": testSystemId},
		Body:    `{"foo":"bar"}`,
	}

	actual := service.HandleMessage(inbound)
	assert.NoError(t, actual)
	mp.AssertExpectations(t)

	actualLog := hook.LastEntry()
	assert.Equal(t, "Skipping this message because the whitelist is invalid.", actualLog.Message, "log message")
	assert.Equal(t, "error", actualLog.Level.String(), "log level")
}
