-- Tasks are only persisted until the scheduler has successfully submitted
-- them to RabbitMQ. They start in SUBMITTED and then become QUEUED or EXPIRED.
CREATE TABLE tasks (
    id              CHAR(36) NOT NULL,
    task_type       VARCHAR(64) NOT NULL,
    status          VARCHAR(32) NOT NULL,
    scheduled_at    DATETIME(6) NOT NULL,
    payload         JSON NOT NULL,
    created_at      DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at      DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    INDEX idx_tasks_status_scheduled_at (status, scheduled_at),
    CONSTRAINT chk_tasks_status CHECK (status IN ('SUBMITTED', 'QUEUED', 'EXPIRED')),
    CONSTRAINT chk_tasks_type CHECK (task_type IN ('SMS_CALLBACK', 'EMAIL_CALLBACK'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
