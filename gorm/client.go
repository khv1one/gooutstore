package gorm

import (
	"context"
	"fmt"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/khv1one/gooutstore"
)

type transactionKey struct{}

type Client struct {
	db                *gorm.DB
	customFromContext func(ctx context.Context) *gorm.DB

	tableName string
}

func NewClient(db *gorm.DB) *Client {
	return &Client{
		tableName: "outbox_messages",
		db:        db,
	}
}

func (c *Client) ReadBatch(ctx context.Context, messageType string, batchSize int) ([]gooutstore.Message, error) {
	rows, err := c.FromContext(ctx).Table(c.tableName).
		Where("status IN (?, ?)", gooutstore.Pending, gooutstore.Retrying).
		Where("message_type = ?", messageType).
		Select("id", "aggregate_key", "status", "retry_count", "body").
		Order("id ASC").
		Clauses(clause.Locking{Strength: clause.LockingStrengthUpdate, Options: clause.LockingOptionsSkipLocked}).
		Rows()
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

	return messages, nil
}

func (c *Client) SetDone(ctx context.Context, m gooutstore.Message) error {
	q := fmt.Sprintf("UPDATE %s SET status = ? WHERE id = ?", c.tableName)

	return c.FromContext(ctx).Exec(q, gooutstore.Done, m.ID).Error
}

func (c *Client) IncRetry(ctx context.Context, m gooutstore.Message) error {
	q := fmt.Sprintf("UPDATE %s SET status = ?, retry_count=retry_count+1 WHERE id = ?", c.tableName)

	return c.FromContext(ctx).Exec(q, gooutstore.Retrying, m.ID).Error
}

func (c *Client) SetBroken(ctx context.Context, m gooutstore.Message) error {
	q := fmt.Sprintf("UPDATE %s SET status = ?, retry_count=retry_count+1 WHERE id = ?", c.tableName)

	return c.FromContext(ctx).Exec(q, gooutstore.Broken, m.ID).Error
}

func (c *Client) Create(ctx context.Context, messages []gooutstore.Message) error {
	var tx *gorm.DB
	if c.customFromContext != nil {
		tx = c.customFromContext(ctx)
	} else {
		tx = c.FromContext(ctx)
	}

	valueStrings := make([]string, 0, len(messages))
	valueArgs := make([]interface{}, 0, len(messages)*5)
	for _, m := range messages {
		valueStrings = append(valueStrings, "(?, ?, ?, ?, ?)")
		valueArgs = append(valueArgs, m.MessageType)
		valueArgs = append(valueArgs, m.AggregateKey)
		valueArgs = append(valueArgs, 0)
		valueArgs = append(valueArgs, 0)
		valueArgs = append(valueArgs, m.Body)
	}

	q := fmt.Sprintf("INSERT INTO %s (message_type, aggregate_key, status, retry_count, body) VALUES %s", c.tableName, strings.Join(valueStrings, ","))
	err := tx.Exec(q, valueArgs...).Error
	return err
}

func (c *Client) FromContext(ctx context.Context) *gorm.DB {
	if tx, ok := ctx.Value(transactionKey{}).(*gorm.DB); ok {
		return tx
	}

	return c.db
}

func (c *Client) ToContext(ctx context.Context, tx *gorm.DB) context.Context {
	return context.WithValue(ctx, transactionKey{}, tx)
}

func (c *Client) WithTransaction(ctx context.Context, fn func(context.Context) error) (err error) {
	if _, ok := ctx.Value(transactionKey{}).(*gorm.DB); ok {
		return fn(ctx)
	}

	tx := c.db.Begin()
	ctx = c.ToContext(ctx, tx)

	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}

		if err != nil {
			tx.Rollback()

			return
		}

		if err = tx.Commit().Error; err != nil {
			return
		}
	}()

	return fn(ctx)
}
