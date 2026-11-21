package rabbitmq

import (
	"encoding/json"
	"fmt"
	"scheduled-pipeline/internal/tasks"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

const (
	SMSCallbackQueue   = "sms_callback"
	EmailCallbackQueue = "email_callback"

	DeadLetterExchange   = "task_dlx"
	DeadLetterQueue      = "task_dlq"
	DeadLetterRoutingKey = "dead"
)

type Message struct {
	ID          string          `json:"id"`
	TaskType    tasks.Type      `json:"task_type"`
	ScheduledAt time.Time       `json:"scheduled_at"`
	ExpiresAt   time.Time       `json:"expires_at"`
	Payload     json.RawMessage `json:"payload"`
}

var mainQueueArgs = amqp091.Table{
	"x-queue-type":         amqp091.QueueTypeQuorum,
	"x-delayed-retry-type": "all",
	"x-delayed-retry-min":  int32(2000),
	"x-delayed-retry-max":  int32(30000),

	// Dead-letter configuration.
	"x-dead-letter-exchange":    DeadLetterExchange,
	"x-dead-letter-routing-key": DeadLetterRoutingKey,

	// Use at-least-once dead-lettering for quorum queues.
	"x-dead-letter-strategy": "at-least-once",
	"x-overflow":              "reject-publish",
}

var dlqArgs = amqp091.Table{
	"x-queue-type": amqp091.QueueTypeQuorum,
}

func setupTopology(channel *amqp091.Channel) error {
	
	if err := channel.ExchangeDeclare(
		DeadLetterExchange,
		amqp091.ExchangeDirect,
		true,  // durable
		false, // auto-delete
		false, // internal
		false, // no-wait
		nil,
	); err != nil {
		return fmt.Errorf("declare dead-letter exchange: %w", err)
	}

	// 2. Declare the dead-letter queue.
	if _, err := channel.QueueDeclare(
		DeadLetterQueue,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		dlqArgs,
	); err != nil {
		return fmt.Errorf("declare dead-letter queue: %w", err)
	}

	// 3. Bind the DLQ to the DLX.
	if err := channel.QueueBind(
		DeadLetterQueue,
		DeadLetterRoutingKey,
		DeadLetterExchange,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("bind dead-letter queue: %w", err)
	}

	// 4. Declare the main task queues.
	for _, queue := range []string{
		SMSCallbackQueue,
		EmailCallbackQueue,
	} {
		if _, err := channel.QueueDeclare(
			queue,
			true,  // durable
			false, // auto-delete
			false, // exclusive
			false, // no-wait
			mainQueueArgs,
		); err != nil {
			return fmt.Errorf("declare queue %q: %w", queue, err)
		}
	}

	return nil
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

func SetupTopology(connection *amqp091.Connection) error {
    channel, err := connection.Channel()
    if err != nil {
        return fmt.Errorf("open topology channel: %w", err)
    }
    defer channel.Close()

    return setupTopology(channel)
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