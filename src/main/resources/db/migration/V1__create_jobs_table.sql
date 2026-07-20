CREATE TABLE jobs (
    id BIGSERIAL PRIMARY KEY,
    job_type VARCHAR(255) NOT NULL,
    payload JSONB,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    priority INT NOT NULL DEFAULT 0,
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempts INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 5,
    last_error TEXT,
    locked_by VARCHAR(255),
    locked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_jobs_polling ON jobs (status, priority DESC, scheduled_at ASC);
CREATE INDEX idx_jobs_type ON jobs (job_type);
