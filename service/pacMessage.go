package service

// MetadataPublishEvent models the events we process from the queue
type PacMetadataPublishEvent struct {
	UUID        string                  `json:"uuid"`
	Annotations []PacMetadataAnnotation `json:"annotations"`
}

type PacMetadataAnnotation struct {
	Predicate string `json:"predicate"`
	ConceptId string `json:"id"`
}
