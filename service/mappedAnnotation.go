package service

// MappedAnnotations are submitted to the writer topic
type MappedAnnotations struct {
	UUID        string       `json:"uuid"`
	Annotations []annotation `json:"annotations"`
}

type annotation struct {
	Concept      concept        `json:"thing"`
}

type concept struct {
	ID        string   `json:"id"`
	Predicate string   `json:"predicate"`
}
