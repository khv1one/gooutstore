package gooutstore

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"
)

type OrderMsg struct {
	OrderID     int
	Description string
}

func (m OrderMsg) Key() string {
	return strconv.Itoa(m.OrderID)
}

func (m OrderMsg) Type() string {
	return "OrderMsg"
}

func TestDispatcher_Start(t *testing.T) {
	testMsg := OrderMsg{}

	tests := []struct {
		name      string
		ctx       context.Context
		setupMock func(context.Context, *clientMock)
		wantErr   bool
	}{
		{
			name: "start success",
			ctx:  context.Background(),
			setupMock: func(ctx context.Context, client *clientMock) {
				client.on("ReadBatch", 1, ctx, testMsg.Type(), 0).andReturn([]Message{}, nil)
			},
			wantErr: false,
		},
		{
			name: "first read error",
			ctx:  context.Background(),
			setupMock: func(ctx context.Context, client *clientMock) {
				client.on("ReadBatch", 1, ctx, testMsg.Type(), 0).andReturn(nil, errors.New("db error"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newClientMock(t)
			tt.setupMock(tt.ctx, client)

			d := &Dispatcher[OrderMsg]{client: client, messageType: testMsg.Type(), interval: 1 * time.Second}

			err := d.Start(tt.ctx)
			_ = d.Stop(tt.ctx)

			if (err != nil) != tt.wantErr {
				t.Errorf("Start() error = %v, wantErr %v", err, tt.wantErr)
			}

			client.assert()
		})
	}
}

func TestDispatcher_Name(t *testing.T) {
	testCases := []struct {
		name     string
		dispName string
		want     string
	}{
		{
			name:     "returns dispatcher name",
			dispName: "test-dispatcher",
			want:     "test-dispatcher",
		},
		{
			name:     "returns empty string when no name set",
			dispName: "",
			want:     "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			d := &Dispatcher[IOutboxMessage]{
				name: tc.dispName,
			}

			got := d.Name()
			if got != tc.want {
				t.Errorf("Name() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDispatcher_processError(t *testing.T) {
	testMsg := OrderMsg{}

	tests := []struct {
		name      string
		ctx       context.Context
		message   Message
		maxRetry  int
		setupMock func(ctx context.Context, client *clientMock, message Message)
	}{
		{
			name: "message retry count exceeds max retry, set broken",
			ctx:  context.Background(),
			message: Message{
				RetryCount: 5,
			},
			maxRetry: 3,
			setupMock: func(ctx context.Context, client *clientMock, message Message) {
				client.on("SetBroken", 1, ctx, message).andReturn(nil)
			},
		},
		{
			name: "message retry count below max retry, increment retry",
			ctx:  context.Background(),
			message: Message{
				RetryCount: 2,
			},
			maxRetry: 3,
			setupMock: func(ctx context.Context, client *clientMock, message Message) {
				client.on("IncRetry", 1, ctx, message).andReturn(nil)
			},
		},
		{
			name: "error on set broken",
			ctx:  context.Background(),
			message: Message{
				RetryCount: 5,
			},
			maxRetry: 3,
			setupMock: func(ctx context.Context, client *clientMock, message Message) {
				client.on("SetBroken", 1, ctx, message).andReturn(nil)
			},
		},
		{
			name: "error on increment retry",
			ctx:  context.Background(),
			message: Message{
				RetryCount: 2,
			},
			maxRetry: 3,
			setupMock: func(ctx context.Context, client *clientMock, message Message) {
				client.on("IncRetry", 1, ctx, message).andReturn(errors.New("increment retry error"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newClientMock(t)
			tt.setupMock(tt.ctx, client, tt.message)

			d := &Dispatcher[OrderMsg]{
				client:      client,
				maxRetry:    tt.maxRetry,
				messageType: testMsg.Type(),
			}

			d.processError(tt.ctx, tt.message)

			client.assert()
		})
	}
}
func TestDispatcher_processMessage(t *testing.T) {
	testMsg := OrderMsg{
		OrderID:     1,
		Description: "Test Order",
	}

	var (
		errSetDone = errors.New("set done error")
		errCall    = errors.New("call error")
	)

	tests := []struct {
		name      string
		ctx       context.Context
		message   Message
		setupMock func(ctx context.Context, client *clientMock, call *processingMock, message Message)
		wantErr   error
	}{
		{
			name: "call function error",
			ctx:  context.Background(),
			message: Message{
				Body: []byte(`{"OrderID":1,"Description":"Test Order"}`),
			},
			setupMock: func(ctx context.Context, client *clientMock, call *processingMock, message Message) {
				call.on("Call", 1, ctx, testMsg).andReturn(errCall)
				client.on("IncRetry", 1, ctx, message).andReturn(nil)
			},
			wantErr: errCall,
		},
		{
			name: "set done error",
			ctx:  context.Background(),
			message: Message{
				Body: []byte(`{"OrderID":1,"Description":"Test Order"}`),
			},
			setupMock: func(ctx context.Context, client *clientMock, call *processingMock, message Message) {
				call.on("Call", 1, ctx, testMsg).andReturn(nil)
				client.on("SetDone", 1, ctx, message).andReturn(errSetDone)
			},
			wantErr: errSetDone,
		},
		{
			name: "process message success",
			ctx:  context.Background(),
			message: Message{
				Body: []byte(`{"OrderID":1,"Description":"Test Order"}`),
			},
			setupMock: func(ctx context.Context, client *clientMock, call *processingMock, message Message) {
				call.on("Call", 1, ctx, testMsg).andReturn(nil)
				client.on("SetDone", 1, ctx, message).andReturn(nil)
			},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newClientMock(t)
			call := newProcessingMock(t)
			tt.setupMock(tt.ctx, client, call, tt.message)

			d := &Dispatcher[OrderMsg]{
				client:      client,
				call:        call.Call,
				messageType: testMsg.Type(),
			}

			err := d.processMessage(tt.ctx, tt.message)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("processMessage() error = %v, wantErr %v", err, tt.wantErr)
			}

			client.assert()
			call.assert()
		})
	}
}
func TestDispatcher_dispatching(t *testing.T) {
	testMsg := OrderMsg{
		OrderID:     1,
		Description: "Test Order",
	}

	var (
		errDB = errors.New("db error")
	)

	tests := []struct {
		name      string
		ctx       context.Context
		setupMock func(ctx context.Context, client *clientMock, call *processingMock)
		wantCount int
		wantErr   error
	}{
		{
			name: "dispatching success with no messages",
			ctx:  context.Background(),
			setupMock: func(ctx context.Context, client *clientMock, call *processingMock) {
				client.on("ReadBatch", 1, ctx, testMsg.Type(), 10).andReturn([]Message{}, nil)
			},
			wantCount: 0,
			wantErr:   nil,
		},
		{
			name: "dispatching success with messages",
			ctx:  context.Background(),
			setupMock: func(ctx context.Context, client *clientMock, call *processingMock) {
				messages := []Message{
					{Body: []byte(`{"OrderID":1,"Description":"Test Order"}`), AggregateKey: "1"},
					{Body: []byte(`{"OrderID":2,"Description":"Test Order"}`), AggregateKey: "1"},
				}
				client.on("ReadBatch", 1, ctx, testMsg.Type(), 10).andReturn(messages, nil)
				client.on("SetDone", 2, ctx, messages[0]).andReturn(nil)
				call.on("Call", 2, ctx, testMsg).andReturn(nil)
			},
			wantCount: 2,
			wantErr:   nil,
		},
		{
			name: "dispatching read batch error",
			ctx:  context.Background(),
			setupMock: func(ctx context.Context, client *clientMock, call *processingMock) {
				client.on("ReadBatch", 1, ctx, testMsg.Type(), 10).andReturn(nil, errDB)
			},
			wantCount: 0,
			wantErr:   errDB,
		},
		{
			name: "dispatching process messages error",
			ctx:  context.Background(),
			setupMock: func(ctx context.Context, client *clientMock, call *processingMock) {
				messages := []Message{
					{Body: []byte(`{"OrderID":1,"Description":"Test Order"}`), AggregateKey: "1"},
				}
				client.on("ReadBatch", 1, ctx, testMsg.Type(), 10).andReturn(messages, nil)
				client.on("IncRetry", 1, ctx, messages[0]).andReturn(nil)
				call.on("Call", 1, ctx, testMsg).andReturn(errors.New("processing error"))
			},
			wantCount: 1,
			wantErr:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newClientMock(t)
			call := newProcessingMock(t)
			tt.setupMock(tt.ctx, client, call)

			d := &Dispatcher[OrderMsg]{
				client:      client,
				call:        call.Call,
				messageType: testMsg.Type(),
				batchSize:   10,
			}

			gotCount, _, err := d.dispatching(tt.ctx)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("dispatching() error = %v, wantErr %v", err, tt.wantErr)
			}
			if gotCount != tt.wantCount {
				t.Errorf("dispatching() gotCount = %v, want %v", gotCount, tt.wantCount)
			}

			client.assert()
			call.assert()
		})
	}
}
