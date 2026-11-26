package consumer

import (
	"context"
	"fmt"
	"log"
	"time"

	"scheduled-pipeline/internal/rabbitmq"
	"scheduled-pipeline/internal/tasks"
)

// Worker consumes only the queue named by SubbedTo.
type Worker struct {
	ID       int
	SubbedTo tasks.Type
	consumer Consumer
	logger   *log.Logger
}

type Consumer interface {
	Consume(context.Context, tasks.Type, rabbitmq.Handler) error 
	ConsumeDlq(ctx context.Context, batchSize int, flushInterval time.Duration,handler rabbitmq.DLQBatchHandler) error
}

func NewWorker(id int, subbedTo tasks.Type, client *rabbitmq.Client, logger *log.Logger) Worker {
	return Worker{ID: id, SubbedTo: subbedTo, consumer: client, logger: logger}
}

func (w Worker) Run(ctx context.Context) error {
	return w.consumer.Consume(ctx, w.SubbedTo, w.handler)
}

func (w Worker) handler(_ context.Context, message rabbitmq.Message) error {
	if !message.ExpiresAt.After(time.Now().UTC()) {
		w.logger.Printf("worker=%d subbed_to=%s skipping expired task=%s expires_at=%s", w.ID, w.SubbedTo, message.ID, message.ExpiresAt.Format(time.RFC3339))
		return fmt.Errorf("queue=%s task_=%s skipped due to expiry", w.SubbedTo, message.ID)
	}

	w.logger.Printf("worker=%d subbed_to=%s handling task=%s type=%s payload=%s", w.ID, w.SubbedTo, message.ID, message.TaskType, message.Payload)
	return nil
}