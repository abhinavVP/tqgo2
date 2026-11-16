package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"scheduled-pipeline/internal/tasks"
	"strconv"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

// Handler runs a task after RabbitMQ delivers it
type Handler func(context.Context, Message) error

func (c *Client) Consume(ctx context.Context, taskType tasks.Type, handler Handler) error {
	queue, err := queueFor(taskType)
	if err != nil {
		return err
	}

	channel, err := c.Connection.Channel()
	if err != nil {
		return fmt.Errorf("open consumer channel: %w", err)
	}
	defer channel.Close()

	if err := channel.Qos(1, 0, false); err != nil {
		return fmt.Errorf("set consumer prefetch: %w", err)
	}

	deliveries, err := channel.Consume(
		queue,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("subscribe to queue %q: %w", queue, err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil

		case delivery, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("delivery channel for queue %q closed", queue)
			}

			var message Message
			if err := json.Unmarshal(delivery.Body, &message); err != nil {
				if nackErr := rejectDelivery(
					delivery,
					message,
					"malformed message; discarding",
					false,
				); nackErr != nil {
					return nackErr
				}
				continue
			}

			if err := handler(ctx, message); err != nil {
				if isMaxAttemptsExceeded(delivery.Headers) {
					if err := rejectDelivery(
						delivery,
						message,
						"maximum attempts exceeded; discarding task",
						false,
					); err != nil {
						return err
					}
				} else {
					if err := rejectDelivery(
						delivery,
						message,
						"task processing failed; scheduling retry",
						true,
					); err != nil {
						return err
					}
				}
				continue
			}

			if err := ackDelivery(
				delivery,
				message,
				"task processed successfully",
				false,
			); err != nil {
				return err
			}
		}
	}
}


type DLQBatchHandler func(context.Context, []amqp091.Delivery) error

func (c *Client) ConsumeDlq(ctx context.Context, batchSize int, flushInterval time.Duration,handler DLQBatchHandler) error {
    channel, err := c.Connection.Channel()
    if err != nil {
        return fmt.Errorf("create DLQ channel: %w", err)
    }
    defer channel.Close()

    if err := channel.Qos(batchSize, 0, false); err != nil {
        return fmt.Errorf("set DLQ QoS: %w", err)
    }

    deliveries, err := channel.Consume(
        DeadLetterQueue, "", false, false, false, false, nil,
    )
    if err != nil {
        return fmt.Errorf("consume DLQ: %w", err)
    }

    batch := make([]amqp091.Delivery, 0, batchSize)
    ticker := time.NewTicker(flushInterval)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return nil

        case delivery, ok := <-deliveries:
            if !ok {
                return fmt.Errorf("DLQ delivery channel closed")
            }

            batch = append(batch, delivery)

            if len(batch) >= batchSize {
                if err := flushBatch(ctx, batch, handler); err != nil {
                    return err
                }
                batch = batch[:0]
            }

        case <-ticker.C:
            if len(batch) == 0 {
                continue
            }

            if err := flushBatch(ctx, batch, handler); err != nil {
                return err
            }
            batch = batch[:0]
        }
    }
}

func flushBatch(ctx context.Context, batch []amqp091.Delivery,handler DLQBatchHandler) error {
    if len(batch) == 0 {
        return nil
    }

    if err := handler(ctx, batch); err != nil {
        last := batch[len(batch)-1]

        if nackErr := last.Nack(true, true); nackErr != nil {
            return fmt.Errorf(
                "archive failed: %v; nack failed: %w",
                err, nackErr,
            )
        }

        return fmt.Errorf("archive DLQ batch: %w", err)
    }

    last := batch[len(batch)-1]
    if err := last.Ack(true); err != nil {
        return fmt.Errorf("ack DLQ batch: %w", err)
    }

    return nil
}

func isMaxAttemptsExceeded(headers amqp091.Table) bool {
	maxRetries := resolveMaxRetryAttempts(4)
	deliveryCount := 0

	if value, ok := headers["x-delivery-count"]; ok {
        deliveryCount = int(value.(int64))
	}

	return deliveryCount >= maxRetries
}

func resolveMaxRetryAttempts(fallback int) int {
	value := os.Getenv("CONSUMER_MAX_RETRIES")
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		log.Fatalf("CONSUMER_MAX_RETRIES must be an integer: %v", err)
	}
	return parsed
}

func ackDelivery(delivery amqp091.Delivery, message Message, logMessage string, requeue bool) error {
    if err := delivery.Ack(requeue); err != nil {
        return fmt.Errorf("acknowledge task error for %s: %w", message.ID, err)
    }

    if logMessage != "" {
        log.Printf("%s: task %s", logMessage, message.ID)
    }

    return nil
}

func rejectDelivery(delivery amqp091.Delivery, message Message, logMessage string, requeu bool) error {
    if err := delivery.Reject(requeu); err != nil {
        return fmt.Errorf("reject task error for %s: %w", message.ID, err)
    }

    if logMessage != "" {
        log.Printf("%s: task %s", logMessage, message.ID)
    }

    return nil
}