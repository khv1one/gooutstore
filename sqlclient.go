package gooutstore

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type sqlClient struct {
	db *sql.DB

	tableName string
}

func newSQLClient(tableName string, db *sql.DB) *sqlClient {
	return &sqlClient{
		tableName: tableName,
		db:        db,
	}
}

func (c *sqlClient) ReadBatch(ctx context.Context, messageType string, batchSize int) ([]Message, error) {
	q := fmt.Sprintf("SELECT id, aggregate_key, status, retry_count, body FROM %s WHERE status IN ($1, $2) AND message_type=$3 ORDER BY id LIMIT $4 FOR UPDATE", c.tableName)
	rows, err := c.db.QueryContext(ctx, q, Pending, Retrying, messageType, batchSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []Message

	// Loop through rows, using Scan to assign column data to struct fields.
	for rows.Next() {
		m := Message{MessageType: messageType}
		if err := rows.Scan(&m.ID, &m.AggregateKey, &m.Status,
			&m.RetryCount, &m.Body); err != nil {
			return messages, err
		}
		messages = append(messages, m)
	}
	if err = rows.Err(); err != nil {
		return messages, err
	}

	return messages, nil
}

func (c *sqlClient) MarkDone(ctx context.Context, m Message) error {
	q := fmt.Sprintf("UPDATE %s SET status = $2 WHERE id = $1", c.tableName)
	_, err := c.db.Exec(q, m.ID, Done)

	return err
}

func (c *sqlClient) MarkRetry(ctx context.Context, m Message) error {
	q := fmt.Sprintf("UPDATE %s SET status = $2, retry_count=retry_count+1 WHERE id = $1", c.tableName)
	_, err := c.db.Exec(q, m.ID, Retrying)
	return err
}

func (c *sqlClient) MarkBroken(ctx context.Context, m Message) error {
	q := fmt.Sprintf("UPDATE %s SET status = $2, retry_count=retry_count+1 WHERE id = $1", c.tableName)
	_, err := c.db.Exec(q, m.ID, Broken)
	return err
}

func (c *sqlClient) Create(ctx context.Context, tx *sql.Tx, messages []Message) error {
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
	//q := fmt.Sprintf("INSERT INTO outbox_messages (message_type, aggregate_key, status, retry_count, body) VALUES %s", strings.Join(valueStrings, ","))
	//q := "INSERT INTO outbox_messages (message_type, aggregate_key, status, retry_count, body) VALUES ($1, $2, $3, $4, $5)"
	//q := `INSERT INTO outbox_messages (message_type, aggregate_key, status, retry_count, body) VALUES ('gggg', 'ggg', 0, 0, '{"OrderID":777,"Description":"olololo"}');`
	_, err := tx.ExecContext(ctx, q, valueArgs...)
	return err
}
