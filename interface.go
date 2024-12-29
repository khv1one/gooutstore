package gooutstore

// IOutboxMessage defines the interface for an outbox message.
type IOutboxMessage interface {
	Key() string
	Type() string
}
