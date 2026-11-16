package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"

	"scheduled-pipeline/internal/consumers"
	"scheduled-pipeline/internal/rabbitmq"
)

func main() {
	workerCount := flag.Int("workers", 0, "positive, even number of workers")
	flag.Parse()
	if err := godotenv.Load(); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Fatalf("load .env: %v", err)
	}
	amqpURL := os.Getenv("AMQP_URL")
	if amqpURL == "" {
		log.Fatal("AMQP_URL is required; set it in .env")
	}
	broker, err := rabbitmq.Connect(amqpURL)
	if err != nil {
		log.Fatalf("rabbitmq setup: %v", err)
	}
	defer broker.Close()

	if err := rabbitmq.SetupTopology(broker.Connection); err != nil {
    	log.Fatalf("rabbitmq topology setup: %v", err)
	}

	pool, err := consumers.NewPool(*workerCount, broker, log.Default())
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Printf("starting %d workers: %d SMS, %d email, 1 dlq", *workerCount+1, *workerCount/2, *workerCount/2)
	if err := pool.Run(ctx); err != nil {
		log.Fatalf("worker pool: %v", err)
	}
}
