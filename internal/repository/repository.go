package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"scheduled-pipeline/internal/tasks"
)

// TaskRepository is the small dependency required by the HTTP API.
type TaskRepository interface {
	Create(context.Context, tasks.Task) error
}

// MySQLTaskRepository contains all MySQL queries for tasks.
type MySQLTaskRepository struct{ db *sql.DB }

func NewMySQLTaskRepository(db *sql.DB) *MySQLTaskRepository { return &MySQLTaskRepository{db: db} }

func (r *MySQLTaskRepository) Create(ctx context.Context, task tasks.Task) error {
	const query = `INSERT INTO tasks
		(id, task_type, status, scheduled_at, expires_at, payload, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	_, err := r.db.ExecContext(ctx, query, task.ID, task.TaskType, task.Status,
		task.ScheduledAt, task.ExpiresAt, task.Payload, task.CreatedAt, task.UpdatedAt)
	if err != nil {
		return fmt.Errorf("insert task: %w", err)
	}
	return nil
}

// FindDue returns submitted tasks that are due and have not yet expired.
func (r *MySQLTaskRepository) FindDue(ctx context.Context, now time.Time, limit int) ([]tasks.Task, error) {
	const query = `SELECT id, task_type, status, scheduled_at, expires_at, payload, created_at, updated_at
		FROM tasks
		WHERE status = ? AND scheduled_at <= ? AND expires_at > ?
		ORDER BY scheduled_at, id
		LIMIT ?`
	rows, err := r.db.QueryContext(ctx, query, tasks.Submitted, now.UTC(), now.UTC(), limit)
	if err != nil {
		return nil, fmt.Errorf("find due tasks: %w", err)
	}
	defer rows.Close()

	var found []tasks.Task
	for rows.Next() {
		var task tasks.Task
		if err := rows.Scan(&task.ID, &task.TaskType, &task.Status, &task.ScheduledAt, &task.ExpiresAt, &task.Payload, &task.CreatedAt, &task.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan due task: %w", err)
		}
		found = append(found, task)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate due tasks: %w", err)
	}
	return found, nil
}

// MarkQueued updates all successfully published tasks in one statement.
func (r *MySQLTaskRepository) MarkQueued(ctx context.Context, ids []string) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	query := fmt.Sprintf(`UPDATE tasks SET status = ?, updated_at = UTC_TIMESTAMP(6)
		WHERE status = ? AND id IN (%s)`, placeholders)
	args := make([]any, 0, len(ids)+2)
	args = append(args, tasks.Queued, tasks.Submitted)
	for _, id := range ids {
		args = append(args, id)
	}
	result, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("mark tasks queued: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("read queue update result: %w", err)
	}
	return updated, nil
}

// ExpireSubmittedAtOrBefore marks submitted tasks whose execution deadline has passed.
func (r *MySQLTaskRepository) ExpireSubmittedAtOrBefore(ctx context.Context, now time.Time) (int64, error) {
	const query = `UPDATE tasks SET status = ?, updated_at = UTC_TIMESTAMP(6)
		WHERE status = ? AND expires_at <= ?`
	result, err := r.db.ExecContext(ctx, query, tasks.Expired, tasks.Submitted, now.UTC())
	if err != nil {
		return 0, fmt.Errorf("expire submitted tasks: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("read expiry update result: %w", err)
	}
	return updated, nil
}
