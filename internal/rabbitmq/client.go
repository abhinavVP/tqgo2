package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"

	"scheduled-pipeline/internal/tasks"
)

const (
	SMSCallbackQueue   = "sms_callback"
	EmailCallbackQueue = "email_callback"
)

// Client owns one RabbitMQ connection. It publishes durably and waits for a
// broker confirmation before returning success to the scheduler.
type Client struct {
	connection *amqp091.Connection
	mu         sync.Mutex
}

func Connect(url string) (*Client, error) {
	connection, err := amqp091.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("connect to rabbitmq: %w", err)
	}
	return &Client{connection: connection}, nil
}

func (c *Client) Close() error { return c.connection.Close() }

// Message is the payload delivered to a worker.
type Message struct {
	ID          string          `json:"id"`
	TaskType    tasks.Type      `json:"task_type"`
	ScheduledAt time.Time       `json:"scheduled_at"`
	ExpiresAt   time.Time       `json:"expires_at"`
	Payload     json.RawMessage `json:"payload"`
}

var queueArgs = amqp091.Table{
	"x-queue-type":         amqp091.QueueTypeQuorum,
	"x-delayed-retry-type": "all",
	"x-delayed-retry-min":  int32(2000),
	"x-delayed-retry-max":  int32(30000),
}

// Enqueue publishes task to its type-specific durable queue. It is serialized
// because AMQP channels are not safe for concurrent use.
func (c *Client) Enqueue(ctx context.Context, task tasks.Task) error {
	queue, err := queueFor(task.TaskType)
	if err != nil {
		return err
	}
	body, err := json.Marshal(Message{ID: task.ID, TaskType: task.TaskType, ScheduledAt: task.ScheduledAt, ExpiresAt: task.ExpiresAt, Payload: task.Payload})
	if err != nil {
		return fmt.Errorf("marshal task message: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	channel, err := c.connection.Channel()
	if err != nil {
		return fmt.Errorf("open rabbitmq channel: %w", err)
	}
	defer channel.Close()
	if _, err := channel.QueueDeclare(queue, true, false, false, false, queueArgs); err != nil {
		return fmt.Errorf("declare queue %q: %w", queue, err)
	}
	if err := channel.Confirm(false); err != nil {
		return fmt.Errorf("enable publish confirmations: %w", err)
	}
	confirmed := channel.NotifyPublish(make(chan amqp091.Confirmation, 1))
	if err := channel.PublishWithContext(ctx, "", queue, false, false, amqp091.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp091.Persistent,
		MessageId:    task.ID,
		Timestamp:    time.Now().UTC(),
		Body:         body,
	}); err != nil {
		return fmt.Errorf("publish task %s: %w", task.ID, err)
	}

	select {
	case confirmation := <-confirmed:
		if !confirmation.Ack {
			return fmt.Errorf("rabbitmq did not confirm task %s", task.ID)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("wait for publish confirmation: %w", ctx.Err())
	}
}

// Handler runs a task after RabbitMQ delivers it. Returning nil acknowledges
// the message; returning an error requeues it for another worker.
type Handler func(context.Context, Message) error

// Consume keeps one AMQP channel subscribed to the queue for taskType. Each
// worker receives at most one unacknowledged message at a time.
func (c *Client) Consume(ctx context.Context, taskType tasks.Type, handler Handler) error {
	queue, err := queueFor(taskType)
	if err != nil {
		return err
	}
	channel, err := c.connection.Channel()
	if err != nil {
		return fmt.Errorf("open consumer channel: %w", err)
	}
	defer channel.Close()
	if _, err := channel.QueueDeclare(queue, true, false, false, false, queueArgs); err != nil {
		return fmt.Errorf("declare queue %q: %w", queue, err)
	}
	if err := channel.Qos(1, 0, false); err != nil {
		return fmt.Errorf("set consumer prefetch: %w", err)
	}
	deliveries, err := channel.Consume(queue, "", false, false, false, false, nil)
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
				if nackErr := rejectDelivery(delivery, message, "malformed message; discarding", false,); nackErr != nil {
					return nackErr
				}
				continue
			}

			
			if err := handler(ctx, message); err != nil {
				if isMaxAttemptsExceeded(delivery.Headers) {
					if err := rejectDelivery(delivery, message, "maximum attempts exceeded; discarding task", false); 
					err != nil {
						return err
					}
				} else {
					if err := rejectDelivery(delivery, message, "task processing failed; scheduling retry", true); 
					err != nil {
						return err
					}
				}
				continue
			}

			if err := ackDelivery(delivery, message, "task processed successfully", false); err != nil {
				return err
			}
		}
	}
}

func queueFor(taskType tasks.Type) (string, error) {
	switch taskType {
	case tasks.SMSCallback:
		return SMSCallbackQueue, nil
	case tasks.EmailCallback:
		return EmailCallbackQueue, nil
	default:
		return "", fmt.Errorf("no queue configured for task type %q", taskType)
	}
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