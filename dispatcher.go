package gooutstore

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// IOutboxDispatcherClient TODO
type IOutboxDispatcherClient interface {
	ReadBatch(ctx context.Context, messageType string) ([]Message, error)
	MarkRetry(ctx context.Context, m Message) error
	MarkBroken(ctx context.Context, m Message) error
	MarkDone(ctx context.Context, m Message) error
}

// Logger TODO
type Logger func(...interface{})

// DispatcherOption TODO
type DispatcherOption[T IOutboxMessage] func(*Dispatcher[T])

// WithMessageType message type for dispatcher instance
func WithMessageType[T IOutboxMessage](messageType string) DispatcherOption[T] {
	return func(d *Dispatcher[T]) { d.messageType = messageType }
}

// WithMaxRetry TODO
func WithMaxRetry[T IOutboxMessage](maxRetry int) DispatcherOption[T] {
	return func(d *Dispatcher[T]) { d.maxRetry = maxRetry }
}

// WithInterval TODO
func WithInterval[T IOutboxMessage](interval time.Duration) DispatcherOption[T] {
	return func(d *Dispatcher[T]) { d.interval = interval }
}

// WithCallback TODO
func WithCallback[T IOutboxMessage](f func(context.Context, T) error) DispatcherOption[T] {
	return func(d *Dispatcher[T]) { d.call = f }
}

// WithDBClient TODO
func WithDBClient[T IOutboxMessage](client IOutboxDispatcherClient) DispatcherOption[T] {
	return func(d *Dispatcher[T]) { d.client = client }
}

// WithErrorLogger TODO
func WithErrorLogger[T IOutboxMessage](logger Logger) DispatcherOption[T] {
	return func(d *Dispatcher[T]) { d.errorLogger = logger }
}

// Dispatcher TODO
type Dispatcher[T IOutboxMessage] struct {
	call func(context.Context, T) error

	client    IOutboxDispatcherClient
	tableName string
	batchSize int
	maxRetry  int
	interval  time.Duration

	name        string
	messageType string

	stop   context.CancelFunc
	stopWG sync.WaitGroup

	errorLogger Logger
}

func NewDispatcher[T IOutboxMessage](opts ...DispatcherOption[T]) *Dispatcher[T] {
	const (
		defaultBatchSize = 100
		defaultMaxRetry  = 0
		defaultInterval  = 1 * time.Second
		defaultTableName = "outbox_messages"
		defaultName      = "outbox-dispatcher-process"
	)

	d := &Dispatcher[T]{
		tableName: defaultTableName,
		batchSize: defaultBatchSize,
		maxRetry:  defaultMaxRetry,
		interval:  defaultInterval,
		name:      defaultName,
	}

	for _, opt := range opts {
		opt(d)
	}

	return d
}

// Start TODO
func (d *Dispatcher[T]) Start(startCtx context.Context) error {
	processCtx := d.initProcessCtx()

	firstRun := make(chan error)
	d.stopWG.Add(1)

	var once sync.Once
	timer := time.NewTimer(0)
	ctx := startCtx

	go func() {
		for {
			select {
			case <-timer.C:
				count, allErr, readErr := d.dispatching(ctx)
				once.Do(func() {
					firstRun <- readErr
					ctx = processCtx
					close(firstRun)
					return
				})

				nextInterval := d.interval
				if readErr != nil && count >= d.batchSize && !allErr {
					nextInterval = 0
				}
				timer.Reset(nextInterval)

			case <-ctx.Done():
				d.stopWG.Done()
				return
			}
		}
	}()

	return <-firstRun
}

// Stop TODO
func (d *Dispatcher[T]) Stop(_ context.Context) error {
	d.stop()
	d.stopWG.Wait()
	// TODO add stopdelay
	return nil
}

// Name TODO
func (d *Dispatcher[T]) Name() string {
	return d.name
}

func (d *Dispatcher[T]) initProcessCtx() context.Context {
	ctx, cancelFunc := context.WithCancel(context.Background())
	d.stop = cancelFunc

	return ctx
}

func (d *Dispatcher[T]) dispatching(ctx context.Context) (msgCount int, isAllErrors bool, readErr error) {
	messages, readErr := d.client.ReadBatch(ctx, d.messageType)
	if readErr != nil {
		return 0, false, fmt.Errorf("outbox reading batch failed: %w", readErr)
	}

	msgCount = len(messages)
	if msgCount == 0 {
		return 0, false, nil
	}

	groupedMessagesByKey := groupByProperty(messages, func(m Message) string {
		return m.AggregateKey
	})

	errCount := 0
	keysCount := 0
	for _, messagesWithSameKey := range groupedMessagesByKey {
		keysCount++
		if err := d.processMessages(ctx, messagesWithSameKey); err != nil {
			// TODO move error logging here
			errCount++
		}
	}

	return msgCount, errCount >= keysCount, nil
}

func (d *Dispatcher[T]) processMessages(ctx context.Context, messages []Message) error {
	for _, m := range messages {
		if err := d.processMessage(ctx, m); err != nil {
			return err
		}
	}

	return nil
}

func (d *Dispatcher[T]) processMessage(ctx context.Context, m Message) error {
	decodedBody, err := decode[T](m.Body)
	if err != nil {
		d.errorLogger("outbox decode message failed", err.Error())
		d.processError(ctx, m)

		return err
	}

	if err := d.call(ctx, decodedBody); err != nil {
		d.errorLogger("outbox callback function failed", err.Error())
		d.processError(ctx, m)

		return err
	}

	if err := d.client.MarkDone(ctx, m); err != nil {
		d.errorLogger("outbox message was processed successfully, but success mark failed", err.Error())

		return err
	}

	return nil
}

func (d *Dispatcher[T]) processError(ctx context.Context, m Message) {
	var err error
	if m.RetryCount >= d.maxRetry && d.maxRetry > 0 {
		err = d.client.MarkBroken(ctx, m)
	} else {
		err = d.client.MarkRetry(ctx, m)
	}

	if err != nil {
		d.errorLogger("outbox process error failed", err.Error())
	}
}

func groupByProperty[T any, K comparable](items []T, getProperty func(T) K) map[K][]T {
	grouped := make(map[K][]T)

	for _, item := range items {
		key := getProperty(item)
		grouped[key] = append(grouped[key], item)
	}

	return grouped
}
