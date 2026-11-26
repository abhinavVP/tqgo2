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
func (r *MySQLTaskRepository) FindDue(ctx context.Context, limit int, leaseTTL time.Duration) ([]tasks.Task, error) {
	now := time.Now()
    const selectQuery = `
        SELECT id, task_type, status, scheduled_at, expires_at,
               payload, created_at, updated_at
        FROM tasks
        WHERE status = ?
          AND scheduled_at <= ?
          AND expires_at > ?
        ORDER BY scheduled_at, id
        LIMIT ?
        FOR UPDATE SKIP LOCKED`

    tx, err := r.db.BeginTx(ctx, nil)
    if err != nil {
        return nil, fmt.Errorf("begin claim transaction: %w", err)
    }
    defer tx.Rollback()

    rows, err := tx.QueryContext(ctx, selectQuery, tasks.Submitted, now.UTC(), now.UTC(), limit)
    if err != nil {
        return nil, fmt.Errorf("select due tasks: %w", err)
    }

    var found []tasks.Task

    for rows.Next() {
        var task tasks.Task
        if err := rows.Scan(
            &task.ID,
            &task.TaskType,
            &task.Status,
            &task.ScheduledAt,
            &task.ExpiresAt,
            &task.Payload,
            &task.CreatedAt,
            &task.UpdatedAt,
        ); err != nil {
            rows.Close()
            return nil, fmt.Errorf("scan due task: %w", err)
        }
        found = append(found, task)
    }

    if err := rows.Err(); err != nil {
        rows.Close()
        return nil, fmt.Errorf("iterate due tasks: %w", err)
    }

    if err := rows.Close(); err != nil {
        return nil, fmt.Errorf("close due task rows: %w", err)
    }

    if len(found) == 0 {
        if err := tx.Commit(); err != nil {
            return nil, fmt.Errorf("commit empty claim: %w", err)
        }
        return found, nil
    }

    const updateQuery = `UPDATE tasks
        SET status = ?, inflight_expires_at = ?, updated_at = ?
        WHERE id IN (%s)`

    placeholders := strings.TrimSuffix(strings.Repeat("?,", len(found)), ",")

    query := fmt.Sprintf(updateQuery, placeholders)
    leaseExpiry := now.UTC().Add(leaseTTL)

    args := []any{tasks.Inflight, leaseExpiry, now.UTC()}
    for _, task := range found {
        args = append(args, task.ID)
    }

    if _, err := tx.ExecContext(ctx, query, args...); err != nil {
        return nil, fmt.Errorf("mark tasks inflight: %w", err)
    }

    if err := tx.Commit(); err != nil {
        return nil, fmt.Errorf("commit task claims: %w", err)
    }

    for i := range found {
        found[i].Status = tasks.Inflight
    }

    return found, nil
}

// MarkQueued updates all successfully published tasks in one statement.
func (r *MySQLTaskRepository) MarkQueued(ctx context.Context, ids []string) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	query := fmt.Sprintf(`UPDATE tasks SET status = ?, updated_at = UTC_TIMESTAMP(6), inflight_expires_at = NULL
		WHERE status = ? AND id IN (%s)`, placeholders)
	args := make([]any, 0, len(ids)+2)
	args = append(args, tasks.Queued, tasks.Inflight)
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
func (r *MySQLTaskRepository) ExpireSubmittedAtOrBefore(ctx context.Context) (int64, error) {
	now := time.Now()
	const query = `UPDATE tasks SET status = ?, updated_at = UTC_TIMESTAMP(6)
		WHERE status in (?, ?) AND expires_at <= ?`
		
	result, err := r.db.ExecContext(ctx, query, tasks.Expired, tasks.Inflight, tasks.Submitted, now.UTC())
	if err != nil {
		return 0, fmt.Errorf("expire submitted tasks: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("read expiry update result: %w", err)
	}
	return updated, nil
}

func (r *MySQLTaskRepository) ResetNackedTask(ctx context.Context, taskId string) (int64, error){
	now := time.Now()
	const q = `UPDATE tasks SET status = ?, updated_at = UTC_TIMESTAMP(6)
		WHERE id = ? AND expires_at > ? AND status = 'INFLIGHT';`

	res, err := r.db.ExecContext(ctx, q, tasks.Submitted, taskId, now.UTC())
	if err != nil {
		return 0, fmt.Errorf("reset nack'd task status : %w", err)
	}
	updated, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("reset nack'd task status result: %w", err)
	}

	return updated, nil
}

func (r *MySQLTaskRepository) ResetInflightExpired(ctx context.Context) (int64, error) {
	const q = `UPDATE tasks
		SET status = ?, inflight_expires_at = NULL, updated_at = UTC_TIMESTAMP(6)
		WHERE status = ? AND inflight_expires_at <= UTC_TIMESTAMP();`
	res, err := r.db.ExecContext(ctx, q, tasks.Submitted, tasks.Inflight)

	if err != nil {
		return 0, fmt.Errorf("reset inflight expired tasks: %w", err)
	}

	updated, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("reset inflight expired result: %w", err)
	}

	return updated, nil

}