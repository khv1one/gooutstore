package gooutstore

// Status TODO
type Status int

const (
	Pending  Status = iota // Pending TODO
	Retrying               // Retrying TODO
	Done                   // Done TODO
	Broken                 // Broken TODO
)

// Message TODO
type Message struct {
	ID           int64
	MessageType  string
	Body         []byte
	AggregateKey string
	Status       Status
	RetryCount   int
}
