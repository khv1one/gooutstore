package gooutstore

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// IOutboxDispatcherClient defines the interface for the outbox dispatcher client.
type IOutboxDispatcherClient interface {
	ReadBatch(ctx context.Context, messageType string, batchSize int) ([]Message, error)
	SetDone(ctx context.Context, m Message) error
	IncRetry(ctx context.Context, m Message) error
	SetBroken(ctx context.Context, m Message) error

	WithTransaction(ctx context.Context, fn func(context.Context) error) (err error)
}

// Logger is a function type for logging messages.
type Logger func(string, ...interface{})

// DispatcherOption represents a configuration option for the Dispatcher.
type DispatcherOption[T IOutboxMessage] func(*Dispatcher[T])

// WithMaxRetry sets the maximum number of retries for a message.
func WithMaxRetry[T IOutboxMessage](maxRetry int) DispatcherOption[T] {
	return func(d *Dispatcher[T]) { d.maxRetry = maxRetry }
}

// WithBatchSize sets the batch size for reading messages.
func WithBatchSize[T IOutboxMessage](batchSize int) DispatcherOption[T] {
	return func(d *Dispatcher[T]) { d.batchSize = batchSize }
}

// WithInterval sets the interval between dispatching attempts.
func WithInterval[T IOutboxMessage](interval time.Duration) DispatcherOption[T] {
	return func(d *Dispatcher[T]) { d.interval = interval }
}

// WithErrorLogger sets the logger for error messages.
func WithErrorLogger[T IOutboxMessage](logger Logger) DispatcherOption[T] {
	return func(d *Dispatcher[T]) { d.errorLogger = logger }
}

// Dispatcher is responsible for dispatching outbox messages.
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

// NewDispatcher creates a new Dispatcher with the provided options.
func NewDispatcher[T IOutboxMessage](call func(context.Context, T) error, opts ...DispatcherOption[T]) *Dispatcher[T] {
	const (
		defaultBatchSize = 100
		defaultMaxRetry  = 0
		defaultInterval  = 1 * time.Second
		defaultTableName = "outbox_messages"
		defaultName      = "outbox-dispatcher-process"
	)

	var m T
	d := &Dispatcher[T]{
		tableName:   defaultTableName,
		batchSize:   defaultBatchSize,
		maxRetry:    defaultMaxRetry,
		interval:    defaultInterval,
		name:        defaultName,
		call:        call,
		messageType: m.Type(),
	}

	for _, opt := range opts {
		opt(d)
	}

	return d
}

// NewDispatcherWithClient creates a new Dispatcher with the provided client and options.
func NewDispatcherWithClient[T IOutboxMessage](client IOutboxDispatcherClient, call func(context.Context, T) error, opts ...DispatcherOption[T]) *Dispatcher[T] {
	d := NewDispatcher(call, opts...)
	d.client = client

	return d
}

// Start begins the dispatching process.
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
					close(firstRun)

					ctx = processCtx
				})

				nextInterval := d.interval
				if readErr != nil && count >= d.batchSize && !allErr {
					if d.errorLogger != nil {
						d.errorLogger("outbox processing failed", readErr.Error())
					}
					nextInterval = nextInterval * 2
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

// Stop halts the dispatching process.
func (d *Dispatcher[T]) Stop(_ context.Context) error {
	d.stop()
	d.stopWG.Wait()
	// TODO add stopdelay
	return nil
}

// Name returns the name of the dispatcher.
func (d *Dispatcher[T]) Name() string {
	return d.name
}

// initProcessCtx initializes the context for the dispatching process.
func (d *Dispatcher[T]) initProcessCtx() context.Context {
	ctx, cancelFunc := context.WithCancel(context.Background())
	d.stop = cancelFunc

	return ctx
}

// dispatching handles the dispatching of messages.
func (d *Dispatcher[T]) dispatching(ctx context.Context) (msgCount int, isAllErrors bool, readErr error) {
	var (
		errCount  int
		keysCount int
	)

	if err := d.client.WithTransaction(ctx, func(ctx context.Context) error {
		messages, readErr := d.client.ReadBatch(ctx, d.messageType, d.batchSize)
		if readErr != nil {
			return fmt.Errorf("outbox reading batch failed: %w", readErr)
		}

		msgCount = len(messages)
		if msgCount == 0 {
			return nil
		}

		groupedMessagesByKey := groupByProperty(messages, func(m Message) string {
			return m.AggregateKey
		})

		for _, messagesWithSameKey := range groupedMessagesByKey {
			keysCount++
			if err := d.processMessages(ctx, messagesWithSameKey); err != nil {
				errCount++
			}
		}

		return nil
	}); err != nil {
		return 0, false, err
	}

	return msgCount, errCount >= keysCount, nil
}

// processMessages processes a batch of messages.
func (d *Dispatcher[T]) processMessages(ctx context.Context, messages []Message) error {
	for _, m := range messages {
		if err := d.processMessage(ctx, m); err != nil {
			return err
		}
	}

	return nil
}

// processMessage processes a single message.
func (d *Dispatcher[T]) processMessage(ctx context.Context, m Message) error {
	decodedBody, err := decode[T](m.Body)
	if err != nil {
		if d.errorLogger != nil {
			d.errorLogger("outbox decode message failed", err.Error())
		}
		d.processError(ctx, m)

		return err
	}

	if err := d.call(ctx, decodedBody); err != nil {
		d.processError(ctx, m)

		return err
	}

	if err := d.client.SetDone(ctx, m); err != nil {
		if d.errorLogger != nil {
			d.errorLogger("outbox message was processed successfully, but set success status failed", err.Error())
		}

		return err
	}

	return nil
}

// processError handles errors that occur during message processing.
func (d *Dispatcher[T]) processError(ctx context.Context, m Message) {
	var err error
	if m.RetryCount >= d.maxRetry && d.maxRetry > 0 {
		err = d.client.SetBroken(ctx, m)
	} else {
		err = d.client.IncRetry(ctx, m)
	}

	if err != nil && d.errorLogger != nil {
		d.errorLogger("outbox mark message failed", err.Error())
	}
}

// groupByProperty groups items by a specified property.
func groupByProperty[T any, K comparable](items []T, getProperty func(T) K) map[K][]T {
	grouped := make(map[K][]T)

	for _, item := range items {
		key := getProperty(item)
		grouped[key] = append(grouped[key], item)
	}

	return grouped
}
