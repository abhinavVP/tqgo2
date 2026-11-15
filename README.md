# Scheduled task pipeline

The API accepts delayed work and persists it in your local MariaDB. Every task enters as `SUBMITTED`. A later scheduler publishes due, unexpired tasks to RabbitMQ, then updates their status to `QUEUED`. Submitted tasks whose explicit `expires_at` deadline passes become `EXPIRED`. The database has no role after either handoff.

## One-time MariaDB setup

Log in as your local MariaDB administrator (typically `sudo mariadb`) and run:

```bash
sudo mariadb
```

```sql
CREATE DATABASE task_pipeline CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'task_user'@'localhost' IDENTIFIED BY 'choose-a-local-password';
GRANT SELECT, INSERT, UPDATE, DELETE ON task_pipeline.* TO 'task_user'@'localhost';
FLUSH PRIVILEGES;
```

Apply migrations from the project root, in order. Run each only once:

```bash
sudo mariadb task_pipeline < migrations/001_create_tasks.sql
sudo mariadb task_pipeline < migrations/002_add_expired_status.sql
sudo mariadb task_pipeline < migrations/003_add_task_expiry.sql
```

## Start the API

Create your local, untracked configuration file once:

```bash
cp .env.example .env
chmod 600 .env
```

Edit `.env` if you chose a different username, password, host, port, or database. It is loaded automatically whenever the API starts.

```bash
go run ./cmd/api
```

## Submit a task

```bash
curl -X POST http://localhost:8080/v1/tasks \
  -H 'Content-Type: application/json' \
  -d '{
    "task_type": "EMAIL_CALLBACK",
    "scheduled_at": "2026-10-25T09:30:00Z",
    "expires_at": "2026-10-25T09:35:00Z",
    "payload": {
      "callback_url": "https://webhook.example.test/email-events",
      "message_id": "email_01JABC",
      "event": "delivered"
    }
  }'
```

`scheduled_at` is RFC 3339 in UTC (or with an explicit offset). Payload must be a JSON object; its task-type-specific content is intentionally preserved verbatim for the RabbitMQ worker phase.

## Scheduler

The scheduler is a separate process. Each cycle selects at most `SCHEDULER_BATCH_SIZE` submitted tasks where `scheduled_at <= now` and `expires_at > now`, publishes them to the durable `sms_callback` or `email_callback` RabbitMQ queue, then marks confirmed publishes as `QUEUED`. It then marks submitted tasks with `expires_at <= now` as `EXPIRED`.

Set `AMQP_URL` and the scheduler settings in `.env`, then run it separately from the API:

```bash
go run ./cmd/scheduler
```

The publisher waits for a RabbitMQ publisher confirmation before changing a task to `QUEUED`. If RabbitMQ accepts a message but MariaDB is unavailable before the status update, the task may be published again in a later cycle; messages therefore use the task ID as the RabbitMQ message ID, and consumers should treat it as an idempotency key. Workers also check `expires_at` before executing and ACK expired messages without running them.

## Worker pool

The worker process requires a positive, even `-workers` count. It starts half the pool subscribed to `sms_callback` and the other half to `email_callback`:

```bash
go run ./cmd/worker -workers 4
```

Each worker uses manual acknowledgements and a prefetch of one message. It logs a simulated callback execution, then ACKs the message. If a future callback handler returns an error, the message is requeued instead of acknowledged.

Run checks with `go test ./...`.
