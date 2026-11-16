package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/joho/godotenv"

	"scheduled-pipeline/internal/database"
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
	broker, err := rabbitmq.Connect(required("AMQP_URL"))
	if err != nil {
		log.Fatalf("rabbitmq setup: %v", err)
	}
	defer broker.Close()
	service, err := scheduler.New(repository.NewMySQLTaskRepository(db), broker, scheduler.Config{BatchSize: intEnv("SCHEDULER_BATCH_SIZE", 100)})
	if err != nil {
		log.Fatalf("scheduler configuration: %v", err)
	}
	interval := durationEnv("SCHEDULER_INTERVAL", 10*time.Second)
	if interval <= 0 {
		log.Fatal("SCHEDULER_INTERVAL must be positive")
	}

	if err := rabbitmq.SetupTopology(broker.Connection); err != nil {
    	log.Fatalf("rabbitmq topology setup: %v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Print("scheduler starting...") 

	schedulerLoop(ctx, service, interval)
}

func schedulerLoop(ctx context.Context, service *scheduler.Service, interval time.Duration) {
	for {
		if err := service.RunOnce(ctx); err != nil {
			log.Printf("scheduler cycle: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
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