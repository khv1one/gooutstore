package gooutstore

// Status represents the status of an outbox message.
type Status int

const (
	Pending  Status = iota // Pending indicates the message is pending processing.
	Retrying               // Retrying indicates the message is being retried.
	Done                   // Done indicates the message has been processed successfully.
	Broken                 // Broken indicates the message has failed and will not be retried.
)

// Message represents an outbox message.
type Message struct {
	ID           int64  // ID is the unique identifier of the message.
	MessageType  string // MessageType is the type of the message.
	Body         []byte // Body is the content of the message.
	AggregateKey string // AggregateKey is the key used to group related messages.
	Status       Status // Status is the current status of the message.
	RetryCount   int    // RetryCount is the number of times the message has been retried.
}
