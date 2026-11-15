package tasks

import (
	"encoding/json"
	"time"
)

type Type string

const (
	SMSCallback   Type = "SMS_CALLBACK"
	EmailCallback Type = "EMAIL_CALLBACK"
)

func (t Type) Valid() bool {
	return t == SMSCallback || t == EmailCallback
}

type Status string

const (
	Submitted Status = "SUBMITTED"
	Queued    Status = "QUEUED"
	Expired   Status = "EXPIRED"
)

type Task struct {
	ID          string          `json:"id"`
	TaskType    Type            `json:"task_type"`
	Status      Status          `json:"status"`
	ScheduledAt time.Time       `json:"scheduled_at"`
	ExpiresAt   time.Time       `json:"expires_at"`
	Payload     json.RawMessage `json:"payload"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}
