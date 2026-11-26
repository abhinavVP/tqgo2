alter table tasks
add COLUMN inflight_expires_at DATETIME(6) default NULL