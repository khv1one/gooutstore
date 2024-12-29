package gooutstore

import (
	"context"
	"errors"
	"testing"
)

type testGenMessage struct {
	TestInt    int
	TestString string
}

func (m testGenMessage) Key() string {
	return "test_key"
}

func (m testGenMessage) Type() string {
	return "test_type"
}

var (
	errSend = errors.New("send error")
)

func TestGenerator_Send(t *testing.T) {
	tests := []struct {
		name      string
		ctx       context.Context
		messages  []IOutboxMessage
		setupMock func(ctx context.Context, client *clientMock, message []IOutboxMessage)
		wantErr   error
	}{
		{
			name:     "positive",
			ctx:      context.Background(),
			messages: []IOutboxMessage{testGenMessage{1, "filed"}, testGenMessage{2, "filed2"}},
			setupMock: func(ctx context.Context, client *clientMock, messages []IOutboxMessage) {
				client.on("Create", 1, ctx, messages).andReturn(nil)
			},
			wantErr: nil,
		},
		{
			name:     "negative send",
			ctx:      context.Background(),
			messages: []IOutboxMessage{},
			setupMock: func(ctx context.Context, client *clientMock, messages []IOutboxMessage) {
				client.on("Create", 1, ctx, messages).andReturn(errSend)
			},
			wantErr: errSend,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newClientMock(t)
			tt.setupMock(tt.ctx, client, tt.messages)

			g := NewGeneratorWithClient(client)

			if err := g.Send(tt.ctx, tt.messages...); !errors.Is(err, tt.wantErr) {
				t.Errorf("Generator.Send() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
