package publisher

import (
	"context"
	"fmt"
	"log"
	"scheduled-pipeline/internal/rabbitmq"
	"scheduled-pipeline/internal/repository"
	"scheduled-pipeline/internal/tasks"
	"sync"
	"time"
)

type Publisher struct {
	rmqc		*rabbitmq.Client
	cfg 		Config
	seqNoMap	map[uint64]string
	mu 			sync.Mutex
}

type Config struct {
	BatchSize 			int
	TaskLeaseTTL 		time.Duration
	AckTickerDuration	time.Duration
}

func New(rmqc *rabbitmq.Client,cfg Config) *Publisher {
	return &Publisher{
		rmqc: rmqc,
		cfg: cfg,
		seqNoMap: make(map[uint64]string),
	}
}

func (p *Publisher) Enqueue(ctx context.Context, sid string, task tasks.Task) error {
	p.registerSeqNum(task.ID)
	
	if err := p.rmqc.PublishMessage(ctx, task); err != nil {
		return fmt.Errorf("publisher rmq publish task %v : %v", task.ID, err.Error())
	}
	
	return nil
}

func (p *Publisher) registerSeqNum(taskId string){
	seqno := p.rmqc.Channel.GetNextPublishSeqNo()
	p.mu.Lock()
	p.seqNoMap[seqno] = taskId
	p.mu.Unlock()
}

func (p *Publisher) getTaskIdOfSeqNum(seqno uint64) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	value, ok := p.seqNoMap[seqno]
	if !ok {
		return "", fmt.Errorf("could not get taskId of seqno %v: not present in map", seqno)
	}
	delete(p.seqNoMap, seqno)
	return value, nil
} 

func (p *Publisher) AckListenerLoop(ctx context.Context, sid string, repo *repository.MySQLTaskRepository) {
	taskIds := make([]string, 0, p.cfg.BatchSize)
	t := time.NewTicker(p.cfg.AckTickerDuration)
	for {
		select {

		case <- ctx.Done():
			t.Stop()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
			defer cancel()

			if err := markQueued(shutdownCtx, repo, taskIds); err != nil {
				log.Printf("final flush: %v", err)
			}
			return

		case <- t.C:
			err := markQueued(ctx, repo, taskIds)
			if err != nil {
				log.Printf("%v", err)
				continue
			}
			taskIds = taskIds[:0]

		case c := <- p.rmqc.Confirmed:
			seqno := c.DeliveryTag
			tid, err := p.getTaskIdOfSeqNum(seqno)
			if (err != nil) {
				log.Printf("%v", err)
				continue
			}

			if c.Ack {
				taskIds = append(taskIds, tid)
				if len(taskIds) >= p.cfg.BatchSize {
					if err := markQueued(ctx, repo, taskIds); err != nil {
						log.Println(err)
						continue
					}
					taskIds = taskIds[:0]
				}
			} else {
				if err := resetNackedTask(ctx, repo, tid); err != nil {
					log.Println(err)
					continue
				}
			}
		}
	}
}

func markQueued(ctx context.Context, repo *repository.MySQLTaskRepository, tasks []string) error {
	if (len(tasks) > 0) {
		n, err := repo.MarkQueued(ctx, tasks)
		if err != nil {
			return fmt.Errorf("ack listener marking tasks queued: %w", err)
		}
		log.Printf("ack'd and updated status for %v tasks", n)
	}

	return nil
}

func resetNackedTask(ctx context.Context, repo *repository.MySQLTaskRepository, taskId string) error {
	_, err := repo.ResetNackedTask(ctx, taskId)
	return err
}