package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"scheduled-pipeline/internal/tasks"
)

type memoryRepository struct{ task tasks.Task }

func (m *memoryRepository) Create(_ context.Context, task tasks.Task) error {
	m.task = task
	return nil
}

func TestCreateTask(t *testing.T) {
	repo := &memoryRepository{}
	server := New(repo)
	server.now = func() time.Time { return time.Date(2026, 10, 24, 10, 0, 0, 0, time.UTC) }
	request := httptest.NewRequest(http.MethodPost, "/v1/tasks", strings.NewReader(`{"task_type":"EMAIL_CALLBACK","scheduled_at":"2026-10-24T11:00:00Z","expires_at":"2026-10-24T11:05:00Z","payload":{"callback_url":"https://example.test/email/123","event":"delivered"}}`))
	response := httptest.NewRecorder()
	server.Routes().ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if repo.task.Status != tasks.Submitted {
		t.Fatalf("status = %q", repo.task.Status)
	}
}

func TestCreateTaskRejectsUnknownType(t *testing.T) {
	server := New(&memoryRepository{})
	request := httptest.NewRequest(http.MethodPost, "/v1/tasks", strings.NewReader(`{"task_type":"PUSH","scheduled_at":"2099-01-01T00:00:00Z","expires_at":"2099-01-01T00:05:00Z","payload":{}}`))
	response := httptest.NewRecorder()
	server.Routes().ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", response.Code)
	}
}
