-- Rollback audit_logs table creation
DROP INDEX IF EXISTS idx_audit_logs_outcome;
DROP INDEX IF EXISTS idx_audit_logs_user_id;
DROP INDEX IF EXISTS idx_audit_logs_created_at;
DROP TABLE IF EXISTS audit_logs;
