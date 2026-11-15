package tasks

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// CreateRequest is the public API contract. Payload stays task-specific so new
// task types do not force a database schema migration.
type CreateTaskDto struct {
	TaskType    Type            `json:"task_type"`
	ScheduledAt time.Time       `json:"scheduled_at"`
	ExpiresAt   time.Time       `json:"expires_at"`
	Payload     json.RawMessage `json:"payload"`
}

func (r CreateTaskDto) Validate(now time.Time) error {
	if !r.TaskType.Valid() {
		return fmt.Errorf("task_type must be SMS_CALLBACK or EMAIL_CALLBACK")
	}
	if r.ScheduledAt.IsZero() {
		return fmt.Errorf("scheduled_at is required and must be RFC3339")
	}
	if r.ExpiresAt.IsZero() {
		return fmt.Errorf("expires_at is required and must be RFC3339")
	}
	if !r.ExpiresAt.After(r.ScheduledAt) {
		return fmt.Errorf("expires_at must be after scheduled_at")
	}
	if !r.ExpiresAt.After(now.UTC()) {
		return fmt.Errorf("expires_at must be in the future")
	}
	if len(r.Payload) == 0 || !json.Valid(r.Payload) {
		return fmt.Errorf("payload must be valid JSON")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(r.Payload, &object); err != nil || object == nil {
		return fmt.Errorf("payload must be a JSON object")
	}
	return nil
}

func NewTask(request CreateTaskDto, now time.Time) Task {
	return Task{
		ID: uuid.NewString(), TaskType: request.TaskType, Status: Submitted,
		ScheduledAt: request.ScheduledAt.UTC(), ExpiresAt: request.ExpiresAt.UTC(), Payload: request.Payload,
		CreatedAt: now.UTC(), UpdatedAt: now.UTC(),
	}
}
