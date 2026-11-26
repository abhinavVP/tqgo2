package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

const (
    dlqBatchSize     = 100
    dlqFlushInterval = 3*time.Second
    dlqArchivePath   = "./data/dlq.jsonl"
)

type DLQRecord struct {
	MessageID  string         `json:"message_id"`
	Exchange   string         `json:"exchange"`
	RoutingKey string         `json:"routing_key"`
	Headers    amqp091.Table  `json:"headers"`
	Body       json.RawMessage`json:"body"`
	ArchivedAt time.Time      `json:"archived_at"`
}

func (w Worker) RunDlq(ctx context.Context) error {
    if err := os.MkdirAll(filepath.Dir(dlqArchivePath), 0750); err != nil {
        return fmt.Errorf("create DLQ archive directory: %w", err)
    }

    file, err := os.OpenFile(
        dlqArchivePath,
        os.O_CREATE|os.O_APPEND|os.O_WRONLY,
        0600,
    )
    if err != nil {
        return fmt.Errorf("open DLQ archive: %w", err)
    }
    defer file.Close()

    return w.consumer.ConsumeDlq(
        ctx,
        dlqBatchSize,
        dlqFlushInterval,
        func(ctx context.Context, batch []amqp091.Delivery) error {
            return w.archiveBatch(file, batch)
        },
    )
}


func (w Worker) archiveBatch(file *os.File, deliveries []amqp091.Delivery) error {
    var buffer bytes.Buffer

    for _, delivery := range deliveries {
        record := toDLQRecord(delivery)

        data, err := json.Marshal(record)
        if err != nil {
            return fmt.Errorf("marshal DLQ record: %w", err)
        }

        buffer.Write(data)
        buffer.WriteByte('\n')
    }

    n, err := file.Write(buffer.Bytes())
    if err != nil {
        return fmt.Errorf("write DLQ batch: %w", err)
    }
    if n != buffer.Len() {
        return io.ErrShortWrite
    }

    if err := file.Sync(); err != nil {
        return fmt.Errorf("sync DLQ archive: %w", err)
    }

    w.logger.Printf("archived %d DLQ messages", len(deliveries))
    return nil
}

func toDLQRecord(delivery amqp091.Delivery) DLQRecord {
	record := DLQRecord{
		MessageID:  delivery.MessageId,
		Exchange:   delivery.Exchange,
		RoutingKey: delivery.RoutingKey,
		Headers:    delivery.Headers,
		Body:       json.RawMessage(delivery.Body),
		ArchivedAt: time.Now().UTC(),
	}

	return record
}