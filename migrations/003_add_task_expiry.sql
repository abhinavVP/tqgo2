-- Add an explicit execution deadline for each task.
ALTER TABLE tasks
    ADD COLUMN expires_at DATETIME(6) NULL AFTER scheduled_at;

-- Compatibility value for tasks created before expires_at existed.
UPDATE tasks
SET expires_at = scheduled_at + INTERVAL 1 MINUTE
WHERE expires_at IS NULL;

ALTER TABLE tasks
    MODIFY COLUMN expires_at DATETIME(6) NOT NULL,
    ADD CONSTRAINT chk_tasks_expiry_after_schedule
        CHECK (expires_at > scheduled_at),
    DROP INDEX idx_tasks_status_scheduled_at,
    ADD INDEX idx_tasks_status_scheduled_at_expires_at
        (status, scheduled_at, expires_at);
