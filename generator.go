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

// WithGeneratorClient TODO
func WithGeneratorClient(client IOutboxGeneratorClient) GeneratorOption {
	return func(g *Generator) { g.client = client }
}

// Generator TODO
type Generator struct {
	client IOutboxGeneratorClient
}

// NewGeneratorWithClient TODO
func NewGeneratorWithClient(client IOutboxGeneratorClient, opts ...GeneratorOption) *Generator {
	g := NewGenerator(opts...)
	g.client = client

	return g
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
			MessageType:  messages[i].Type(),
			Body:         body,
			AggregateKey: messages[i].Key(),
			Status:       Pending,
		})
	}

	return g.client.Create(ctx, encodeMessages)
}
