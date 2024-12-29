package pgx

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/khv1one/gooutstore"
)

type transactionKey struct{}

// TX defines the interface for a transaction.
type TX interface {
	Query(ctx context.Context, query string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error)
}

// Client represents a pgx client for outbox messages.
type Client struct {
	db                *pgxpool.Pool
	customFromContext func(ctx context.Context) TX

	tableName string
}

// NewClient creates a new pgx client.
func NewClient(db *pgxpool.Pool) *Client {
	return &Client{
		tableName: "outbox_messages",
		db:        db,
	}
}

// ReadBatch reads a batch of messages from the outbox.
func (c *Client) ReadBatch(ctx context.Context, messageType string, batchSize int) ([]gooutstore.Message, error) {
	q := fmt.Sprintf("SELECT id, aggregate_key, status, retry_count, body FROM %s WHERE status IN ($1, $2) AND message_type=$3 ORDER BY id LIMIT $4 FOR UPDATE", c.tableName)
	rows, err := c.FromContext(ctx).Query(ctx, q, gooutstore.Pending, gooutstore.Retrying, messageType, batchSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []gooutstore.Message

	for rows.Next() {
		m := gooutstore.Message{MessageType: messageType}
		if err := rows.Scan(&m.ID, &m.AggregateKey, &m.Status, &m.RetryCount, &m.Body); err != nil {
			return messages, err
		}
		messages = append(messages, m)
	}
	if err = rows.Err(); err != nil {
		return messages, err
	}

	return messages, nil
}

// SetDone marks a message as done.
func (c *Client) SetDone(ctx context.Context, m gooutstore.Message) error {
	q := fmt.Sprintf("UPDATE %s SET status = $2 WHERE id = $1", c.tableName)

	_, err := c.FromContext(ctx).Exec(ctx, q, m.ID, gooutstore.Done)
	return err
}

// IncRetry increments the retry count for a message.
func (c *Client) IncRetry(ctx context.Context, m gooutstore.Message) error {
	q := fmt.Sprintf("UPDATE %s SET status = $2, retry_count=retry_count+1 WHERE id = $1", c.tableName)

	_, err := c.FromContext(ctx).Exec(ctx, q, m.ID, gooutstore.Retrying)
	return err
}

// SetBroken marks a message as broken.
func (c *Client) SetBroken(ctx context.Context, m gooutstore.Message) error {
	q := fmt.Sprintf("UPDATE %s SET status = $2, retry_count=retry_count+1 WHERE id = $1", c.tableName)

	_, err := c.FromContext(ctx).Exec(ctx, q, m.ID, gooutstore.Broken)
	return err
}

// Create creates new outbox messages.
func (c *Client) Create(ctx context.Context, messages []gooutstore.Message) error {
	var tx TX
	if c.customFromContext != nil {
		tx = c.customFromContext(ctx)
	} else {
		tx = c.FromContext(ctx)
	}

	valueStrings := make([]string, 0, len(messages))
	valueArgs := make([]interface{}, 0, len(messages)*5)
	for i, m := range messages {
		valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d)", i*5+1, i*5+2, i*5+3, i*5+4, i*5+5))
		valueArgs = append(valueArgs, m.MessageType)
		valueArgs = append(valueArgs, m.AggregateKey)
		valueArgs = append(valueArgs, 0)
		valueArgs = append(valueArgs, 0)
		valueArgs = append(valueArgs, m.Body)
	}

	q := fmt.Sprintf("INSERT INTO %s (message_type, aggregate_key, status, retry_count, body) VALUES %s", c.tableName, strings.Join(valueStrings, ","))
	_, err := tx.Exec(ctx, q, valueArgs...)
	return err
}

// FromContext retrieves the transaction from the context.
func (c *Client) FromContext(ctx context.Context) TX {
	if tx, ok := ctx.Value(transactionKey{}).(pgx.Tx); ok {
		return tx
	}

	return c.db
}

// ToContext adds the transaction to the context.
func (c *Client) ToContext(ctx context.Context, tx pgx.Tx) context.Context {
	return context.WithValue(ctx, transactionKey{}, tx)
}

// WithTransaction executes a function within a transaction.
func (c *Client) WithTransaction(ctx context.Context, fn func(context.Context) error) (err error) {
	if _, ok := ctx.Value(transactionKey{}).(*sql.Tx); ok {
		return fn(ctx)
	}

	tx, err := c.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction failed: %w", err)
	}

	ctx = c.ToContext(ctx, tx)

	defer func() {
		if p := recover(); p != nil {
			tx.Rollback(ctx)
			err = fmt.Errorf("panic: %s", p)
			return
		}

		if err != nil {
			if rErr := tx.Rollback(ctx); err != nil {
				err = rErr
				return
			}

			return
		}

		if err = tx.Commit(ctx); err != nil {
			return
		}
	}()

	return fn(ctx)
}
