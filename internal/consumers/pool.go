package consumers

import (
	"context"
	"fmt"
	"log"
	"sync"

	"scheduled-pipeline/internal/rabbitmq"
	"scheduled-pipeline/internal/tasks"
)

// Pool starts an equal number of SMS and email workers. Count must be a
// positive even number so neither queue is starved by configuration.
type Pool struct {
	workers []Worker
}

func NewPool(count int, client *rabbitmq.Client, logger *log.Logger) (*Pool, error) {
	if count <= 0 || count%2 != 0 {
		return nil, fmt.Errorf("worker count must be a positive even number")
	}
	workers := make([]Worker, 0, count)
	for id := 1; id <= count/2; id++ {
		workers = append(workers, NewWorker(id, tasks.SMSCallback, client, logger))
	}
	for id := count/2 + 1; id <= count; id++ {
		workers = append(workers, NewWorker(id, tasks.EmailCallback, client, logger))
	}

	workers = append(workers, NewWorker(999, rabbitmq.DeadLetterQueue, client, logger)) //id 999 only for dlq worker (idk what to do here) 
	return &Pool{workers: workers}, nil
}

func (p *Pool) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errs := make(chan error, len(p.workers))
	var group sync.WaitGroup
	for _, worker := range p.workers {
		group.Add(1)
		if (worker.ID == 999) {
			go func (worker Worker) {
				defer group.Done()
				if err := worker.RunDlq(ctx); err != nil {
					errs <- err
				}
			} (worker)
		} else {
			go func(worker Worker) {
				defer group.Done()
				if err := worker.Run(ctx); err != nil {
					errs <- err
				}
			}(worker)
		}
	}
	done := make(chan struct{})
	go func() { group.Wait(); close(done) }()

	select {
	case err := <-errs:
		cancel()
		<-done
		return err
	case <-ctx.Done():
		<-done
	}
	return nil
}
