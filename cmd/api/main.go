package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"

	"scheduled-pipeline/internal/database"
	"scheduled-pipeline/internal/httpapi"
	"scheduled-pipeline/internal/repository"
)

func main() {
	// Load project-local configuration when present. Existing environment
	// variables take precedence, which remains useful for deployment.
	if err := godotenv.Load(); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Fatalf("load .env: %v", err)
	}

	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		log.Fatal("MYSQL_DSN is required; copy .env.example to .env and set it")
	}
	db, err := database.Open(dsn)
	if err != nil {
		log.Fatalf("database setup: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		log.Fatalf("database unavailable: %v", err)
	}

	server := &http.Server{Addr: envOr("HTTP_ADDR", ":8080"), Handler: httpapi.New(repository.NewMySQLTaskRepository(db)).Routes(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Printf("API listening on %s", server.Addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server: %v", err)
		}
	}()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("graceful shutdown: %v", err)
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
