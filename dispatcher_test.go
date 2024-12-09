package gooutstore

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"
)

type DispatcherSuite struct {
}

func DispatcherSuiteInit() DispatcherSuite {
	return DispatcherSuite{}
}

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
			client := NewClientMock(t)
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
			client := NewClientMock(t)
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
