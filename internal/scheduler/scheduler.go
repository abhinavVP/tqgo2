package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"scheduled-pipeline/internal/publisher"
	"scheduled-pipeline/internal/repository"

	"github.com/google/uuid"
)

// Service moves due, unexpired submitted tasks to RabbitMQ, then expires
// submitted tasks whose explicit deadline has passed.
type Service struct {
	id 			string
	repository  *repository.MySQLTaskRepository
	publisher   *publisher.Publisher
}

func New(repository *repository.MySQLTaskRepository, publisher *publisher.Publisher) (*Service, error) {
	id := uuid.NewString()
	return &Service{id: id, repository: repository, publisher: publisher}, nil
}

func (s *Service) RunOnce(ctx context.Context, batchSize int, leaseTTl time.Duration) error {
	due, err := s.repository.FindDue(ctx, batchSize, leaseTTl)
	if err != nil {
		return err
	}
	
	var failures []error
	
	log.Printf("scheduler found %d due submitted job(s)", len(due))
	
	if len(due) > 0 {
		for _, task := range due {
			if err := s.publisher.Enqueue(ctx, s.id, task); err != nil {
				failures = append(failures, fmt.Errorf("enqueue task %s: %w", task.ID, err))
			}
		}
	}

	if expired, err := s.repository.ExpireSubmittedAtOrBefore(ctx); err != nil {
		failures = append(failures, err)
	} else if expired > 0 {
		log.Printf("scheduler marked %d job(s) EXPIRED", expired)
	}

	if inflight_expired, err := s.repository.ResetInflightExpired(ctx); err != nil {
		failures = append(failures, err)
	} else if inflight_expired > 0 {
		log.Printf("scheduler reset %d job(s) to SUBMITTED", inflight_expired)
	}

	return errors.Join(failures...)
}

func (s *Service)SchedulerLoop(ctx context.Context, interval time.Duration, batchSize int, leaseTTL time.Duration) {
	for {
		jitter := time.Duration(rand.Int63n(int64(interval)))
    	timer := time.NewTimer(jitter)
		if err := s.RunOnce(ctx, batchSize, leaseTTL); err != nil {
			log.Printf("scheduler cycle errors: %v", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
	}
}

func (s *Service) AckListenerLoop(ctx context.Context) {
	s.publisher.AckListenerLoop(ctx, s.id, s.repository)
}