-- Create audit_logs table for persistent query logging
-- Per T030: Audit logs must be persisted to PostgreSQL
-- Per phase-4-spec.md §5: Every request MUST log these fields

CREATE TABLE IF NOT EXISTS audit_logs (
    id SERIAL PRIMARY KEY,
    
    -- Required fields per phase-4-spec.md
    query_id VARCHAR(255) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    
    -- Optional fields for authorization and routing
    role VARCHAR(255),
    tables_json JSONB DEFAULT '[]'::jsonb,
    
    -- Authorization and planner decisions
    auth_decision VARCHAR(50),
    planner_decision TEXT,
    
    -- Execution details
    engine VARCHAR(100),
    execution_time_ms BIGINT NOT NULL DEFAULT 0,
    
    -- Outcome tracking
    outcome VARCHAR(50),
    error_message TEXT,
    invariant_violated TEXT,
    
    -- Timestamp
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Indexes for common queries
    CONSTRAINT audit_logs_query_id_unique UNIQUE (query_id)
);

-- Index for time-based queries (audit summary)
CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at ON audit_logs(created_at);

-- Index for user-based queries
CREATE INDEX IF NOT EXISTS idx_audit_logs_user_id ON audit_logs(user_id);

-- Index for outcome filtering
CREATE INDEX IF NOT EXISTS idx_audit_logs_outcome ON audit_logs(outcome);
