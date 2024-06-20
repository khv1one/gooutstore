package gooutstore

type IOutboxMessage interface {
	AggregateKey() string
	MessageKind() string
}
