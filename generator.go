package gooutstore

import (
	"context"
)

// IOutboxGeneratorClient TODO
type IOutboxGeneratorClient interface {
	Create(ctx context.Context, messages []Message) error
}

// GeneratorOption TODO
type GeneratorOption func(*Generator)

// WithGeneratorTableName TODO
func WithGeneratorTableName(tableName string) GeneratorOption {
	return func(g *Generator) { g.tableName = tableName }
}

// WithGeneratorClient TODO
func WithGeneratorClient(client IOutboxGeneratorClient) GeneratorOption {
	return func(g *Generator) { g.client = client }
}

// Generator TODO
type Generator struct {
	client IOutboxGeneratorClient

	tableName string
}

// NewGenerator TODO
func NewGenerator(opts ...GeneratorOption) *Generator {
	g := &Generator{}

	for _, opt := range opts {
		opt(g)
	}

	return g
}

// Send TODO
func (g *Generator) Send(ctx context.Context, messages ...IOutboxMessage) error {
	encodeMessages := make([]Message, 0, len(messages))
	for i := 0; i < len(messages); i++ {
		body, err := encode(messages[i])
		if err != nil {
			return err
		}

		encodeMessages = append(encodeMessages, Message{
			MessageType:  messages[i].MessageKind(),
			Body:         body,
			AggregateKey: messages[i].AggregateKey(),
			Status:       Pending,
		})
	}

	return g.client.Create(ctx, encodeMessages)
}
