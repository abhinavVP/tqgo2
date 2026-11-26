ALTER TABLE tasks DROP CONSTRAINT chk_tasks_status;
ALTER TABLE tasks
    ADD CONSTRAINT chk_tasks_status
    CHECK (status IN ('SUBMITTED', 'QUEUED', 'EXPIRED', 'INFLIGHT'));
