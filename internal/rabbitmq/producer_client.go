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
type Client struct {
	Connection *amqp091.Connection
	Channel	   *amqp091.Channel
	Confirmed  chan amqp091.Confirmation
}

func Connect(url string) (*Client, error) {
	connection, err := amqp091.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("connect to rabbitmq: %w", err)
	}
	c := Client{Connection: connection}
	if err := c.InitChannel(); err != nil{
		return nil, fmt.Errorf("init channel failed: %w", err)
	}

	return &c, nil
}

func (c *Client) Close() error { 
	if err := c.Channel.Close(); err != nil {
		return err
	}
	return c.Connection.Close() 
}

func (c *Client) InitChannel() error {
	channel, err := c.Connection.Channel()
	if err != nil {
		return fmt.Errorf("open rabbitmq channel: %w", err)
	}

	if err := channel.Confirm(false); err != nil {
		return fmt.Errorf("enable publish confirmations: %w", err)
	}

	c.Channel = channel
	c.Confirmed = channel.NotifyPublish(make(chan amqp091.Confirmation, 1))

	return nil
}

// Enqueue publishes task to its type-specific durable queue.
func (c *Client) Enqueue(ctx context.Context, task tasks.Task) error {
	queue, err := queueFor(task.TaskType)
	if err != nil {
		return err
	}

	body, err := taskToMessageJSON(task)

	if err != nil {
		return fmt.Errorf("enqueue message JSON marshal: %v", err)
	}

	if err := c.Channel.PublishWithContext(
		ctx,
		"",
		queue,
		false,
		false,
		resolveMessage(task.ID, body),
	); err != nil {
		return fmt.Errorf("publish task %s: %w", task.ID, err)
	}

	select {
	case confirmation := <-c.Confirmed:
		if !confirmation.Ack {
			return fmt.Errorf("rabbitmq did not confirm task %s", task.ID)
		}
		return nil

	case <-ctx.Done():
		return fmt.Errorf("wait for publish confirmation: %w", ctx.Err())
	}
}

func resolveMessage(taskId string, body []byte) amqp091.Publishing {
	return amqp091.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp091.Persistent,
		MessageId:    taskId,
		Timestamp:    time.Now().UTC(),
		Body:         body,
	}
}

func taskToMessageJSON(t tasks.Task) ([]byte, error) {
	body, err := json.Marshal(Message{
		ID:          t.ID,
		TaskType:    t.TaskType,
		ScheduledAt: t.ScheduledAt,
		ExpiresAt:   t.ExpiresAt,
		Payload:     t.Payload,
	})

	if err != nil {
		return []byte{}, err
	}

	return body, nil
}

func (c * Client) PublishMessage(ctx context.Context, t tasks.Task) error {
	queue, err := queueFor(t.TaskType)
	if err != nil {
		return err
	}

	body, err := taskToMessageJSON(t)

	if err != nil {
		return fmt.Errorf("enqueue message JSON marshal: %v", err)
	}

	if err := c.Channel.PublishWithContext(
		ctx,
		"",
		queue,
		false,
		false,
		resolveMessage(t.ID, body),
	); err != nil {
		return fmt.Errorf("publish task %s: %w", t.ID, err)
	}

	return nil
}

