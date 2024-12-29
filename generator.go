package gooutstore

import (
	"context"
)

// IOutboxGeneratorClient defines the interface for the outbox generator client.
type IOutboxGeneratorClient interface {
	Create(ctx context.Context, messages []Message) error
}

// GeneratorOption represents a configuration option for the Generator.
type GeneratorOption func(*Generator)

// WithGeneratorClient sets the client for the Generator.
func WithGeneratorClient(client IOutboxGeneratorClient) GeneratorOption {
	return func(g *Generator) { g.client = client }
}

// Generator is responsible for generating outbox messages.
type Generator struct {
	client IOutboxGeneratorClient
}

// NewGeneratorWithClient creates a new Generator with the provided client and options.
func NewGeneratorWithClient(client IOutboxGeneratorClient, opts ...GeneratorOption) *Generator {
	g := NewGenerator(opts...)
	g.client = client

	return g
}

// NewGenerator creates a new Generator with the provided options.
func NewGenerator(opts ...GeneratorOption) *Generator {
	g := &Generator{}

	for _, opt := range opts {
		opt(g)
	}

	return g
}

// Send sends the provided messages using the Generator's client.
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
