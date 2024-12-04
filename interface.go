package gooutstore

type IOutboxMessage interface {
	Key() string
	Type() string
}
