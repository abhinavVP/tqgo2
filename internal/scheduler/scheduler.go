package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"scheduled-pipeline/internal/repository"
	"scheduled-pipeline/internal/tasks"
)

type Publisher interface {
	Enqueue(context.Context, tasks.Task) error
}

type Config struct {
	BatchSize int
}

func (c Config) Validate() error {
	if c.BatchSize < 1 {
		return fmt.Errorf("batch size must be at least 1")
	}
	return nil
}

// Service moves due, unexpired submitted tasks to RabbitMQ, then expires
// submitted tasks whose explicit deadline has passed.
type Service struct {
	repository *repository.MySQLTaskRepository
	publisher  Publisher
	config     Config
	now        func() time.Time
}

func New(repository *repository.MySQLTaskRepository, publisher Publisher, config Config) (*Service, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &Service{repository: repository, publisher: publisher, config: config, now: time.Now}, nil
}

// RunOnce is intentionally bounded by BatchSize, so one slow cycle cannot
// monopolize the process. A later cycle processes the next batch.
func (s *Service) RunOnce(ctx context.Context) error {
	now := s.now().UTC()
	due, err := s.repository.FindDue(ctx, now, s.config.BatchSize)
	if err != nil {
		return err
	}
	if len(due) > 0 {
		log.Printf("scheduler found %d due submitted job(s)", len(due))
	}

	var failures []error
	queuedIDs := make([]string, 0, len(due))
	for _, task := range due {
		if err := s.publisher.Enqueue(ctx, task); err != nil {
			failures = append(failures, fmt.Errorf("enqueue task %s: %w", task.ID, err))
			continue
		}
		queuedIDs = append(queuedIDs, task.ID)
	}
	if len(queuedIDs) > 0 {
		log.Printf("scheduler published %d job(s) to RabbitMQ", len(queuedIDs))
		updated, err := s.repository.MarkQueued(ctx, queuedIDs)
		if err != nil {
			failures = append(failures, fmt.Errorf("mark published tasks queued: %w", err))
		} else if updated != int64(len(queuedIDs)) {
			failures = append(failures, fmt.Errorf("marked %d of %d published tasks queued", updated, len(queuedIDs)))
		} else {
			log.Printf("scheduler marked %d job(s) QUEUED", updated)
		}
	}

	if expired, err := s.repository.ExpireSubmittedAtOrBefore(ctx, now); err != nil {
		failures = append(failures, err)
	} else if expired > 0 {
		log.Printf("scheduler marked %d job(s) EXPIRED", expired)
	}
	return errors.Join(failures...)
}
