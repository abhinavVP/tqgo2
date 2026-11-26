package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"

	"scheduled-pipeline/internal/database"
	"scheduled-pipeline/internal/publisher"
	"scheduled-pipeline/internal/rabbitmq"
	"scheduled-pipeline/internal/repository"
	"scheduled-pipeline/internal/scheduler"
)

func main() {
	if err := godotenv.Load(); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Fatalf("load .env: %v", err)
	}
	db, err := database.Open(required("MYSQL_DSN"))
	if err != nil {
		log.Fatalf("database setup: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		log.Fatalf("database unavailable: %v", err)
	}
	interval := durationEnv("SCHEDULER_INTERVAL", 10*time.Second)
	leaseTtl := durationEnv("TASK_LEASE_TTL", 60*time.Second)
	batchSize := intEnv("SCHEDULER_BATCH_SIZE", 100)
	ackTicker := durationEnv("ACK_LISTENER_TICKER", 30*time.Second)

	broker, err := rabbitmq.Connect(required("AMQP_URL"))
	if err != nil {
		log.Fatalf("rabbitmq setup: %v", err)
	}
	defer broker.Close()

	cfg := publisher.Config{
		BatchSize: batchSize,
		TaskLeaseTTL: leaseTtl,
		AckTickerDuration: ackTicker,
	}
	
	pub := publisher.New(
		broker,
		cfg,
	)

	service, err := scheduler.New(
		repository.NewMySQLTaskRepository(db), 
		pub,
	)
	if err != nil {
		log.Fatalf("scheduler configuration: %v", err)
	}

	if err := rabbitmq.SetupTopology(broker.Connection); err != nil {
    	log.Fatalf("rabbitmq topology setup: %v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Print("scheduler starting...") 

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()
		service.SchedulerLoop(ctx, interval, batchSize, leaseTtl)
	}()

	go func() {
		defer wg.Done()
		service.AckListenerLoop(ctx)
	}()

	wg.Wait()
}

func required(key string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	log.Fatalf("%s is required; set it in .env", key)
	return ""
}
func durationEnv(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		log.Fatalf("%s must be a duration: %v", key, err)
	}
	return parsed
}
func intEnv(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		log.Fatalf("%s must be an integer: %v", key, err)
	}
	return parsed
}