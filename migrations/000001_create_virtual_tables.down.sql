-- Rollback: Drop virtual_tables and related tables

DROP TRIGGER IF EXISTS update_virtual_tables_updated_at ON virtual_tables;
DROP FUNCTION IF EXISTS update_updated_at_column();

DROP TABLE IF EXISTS query_audit_log;
DROP TABLE IF EXISTS table_constraints;
DROP TABLE IF EXISTS table_capabilities;
DROP TABLE IF EXISTS physical_sources;
DROP TABLE IF EXISTS virtual_tables;
