package service

// ConceptAnnotations models the annotations as it will be written on the queue
type ConceptAnnotations struct {
	UUID        string       `json:"uuid"`
	Annotations []annotation `json:"annotations"`
}

type annotation struct {
	Thing      thing        `json:"thing"`
}

type thing struct {
	ID        string   `json:"id"`
	Predicate string   `json:"predicate"`
}
