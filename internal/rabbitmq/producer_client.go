package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rabbitmq/amqp091-go"

	"scheduled-pipeline/internal/tasks"
)


// Client owns one RabbitMQ connection. It publishes durably and waits for a
// broker confirmation before returning success to the scheduler.
type ProducerClient struct {
	Connection *amqp091.Connection
}

func ProducerConnect(url string) (*ProducerClient, error) {
	connection, err := amqp091.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("connect to rabbitmq: %w", err)
	}
	return &ProducerClient{Connection: connection}, nil
}

func (c *ProducerClient) Close() error { return c.Connection.Close() }

// Enqueue publishes task to its type-specific durable queue. It is serialized
// because AMQP channels are not safe for concurrent use.
func (c *ProducerClient) Enqueue(ctx context.Context, task tasks.Task) error {
	queue, err := queueFor(task.TaskType)
	if err != nil {
		return err
	}

	body, err := json.Marshal(Message{
		ID:          task.ID,
		TaskType:    task.TaskType,
		ScheduledAt: task.ScheduledAt,
		ExpiresAt:   task.ExpiresAt,
		Payload:     task.Payload,
	})
	if err != nil {
		return fmt.Errorf("marshal task message: %w", err)
	}

	channel, err := c.Connection.Channel()
	if err != nil {
		return fmt.Errorf("open rabbitmq channel: %w", err)
	}
	defer channel.Close()

	if err := channel.Confirm(false); err != nil {
		return fmt.Errorf("enable publish confirmations: %w", err)
	}

	confirmed := channel.NotifyPublish(make(chan amqp091.Confirmation, 1))

	if err := channel.PublishWithContext(
		ctx,
		"",
		queue,
		false,
		false,
		amqp091.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp091.Persistent,
			MessageId:    task.ID,
			Timestamp:    time.Now().UTC(),
			Body:         body,
		},
	); err != nil {
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