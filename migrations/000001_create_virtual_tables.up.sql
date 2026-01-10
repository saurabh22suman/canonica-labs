-- Migration: Create virtual_tables table
-- Description: Core table for storing virtual table definitions

CREATE TABLE IF NOT EXISTS virtual_tables (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Index for name lookups
CREATE INDEX IF NOT EXISTS idx_virtual_tables_name ON virtual_tables(name);

-- Table for physical sources
CREATE TABLE IF NOT EXISTS physical_sources (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    virtual_table_id UUID NOT NULL REFERENCES virtual_tables(id) ON DELETE CASCADE,
    format VARCHAR(50) NOT NULL,
    location TEXT NOT NULL,
    engine VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    
    CONSTRAINT valid_format CHECK (format IN ('DELTA', 'ICEBERG', 'PARQUET'))
);

CREATE INDEX IF NOT EXISTS idx_physical_sources_table ON physical_sources(virtual_table_id);

-- Table for capabilities
CREATE TABLE IF NOT EXISTS table_capabilities (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    virtual_table_id UUID NOT NULL REFERENCES virtual_tables(id) ON DELETE CASCADE,
    capability VARCHAR(50) NOT NULL,
    
    CONSTRAINT valid_capability CHECK (capability IN ('READ', 'TIME_TRAVEL')),
    CONSTRAINT unique_table_capability UNIQUE (virtual_table_id, capability)
);

CREATE INDEX IF NOT EXISTS idx_table_capabilities_table ON table_capabilities(virtual_table_id);

-- Table for constraints
CREATE TABLE IF NOT EXISTS table_constraints (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    virtual_table_id UUID NOT NULL REFERENCES virtual_tables(id) ON DELETE CASCADE,
    constraint_type VARCHAR(50) NOT NULL,
    
    CONSTRAINT valid_constraint CHECK (constraint_type IN ('READ_ONLY', 'SNAPSHOT_CONSISTENT')),
    CONSTRAINT unique_table_constraint UNIQUE (virtual_table_id, constraint_type)
);

CREATE INDEX IF NOT EXISTS idx_table_constraints_table ON table_constraints(virtual_table_id);

-- Audit log for queries
CREATE TABLE IF NOT EXISTS query_audit_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    query_id UUID NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    raw_sql TEXT NOT NULL,
    tables_referenced TEXT[] NOT NULL,
    engine_selected VARCHAR(100),
    execution_time_ms INTEGER,
    error TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_query_audit_log_user ON query_audit_log(user_id);
CREATE INDEX IF NOT EXISTS idx_query_audit_log_created ON query_audit_log(created_at);
CREATE INDEX IF NOT EXISTS idx_query_audit_log_query_id ON query_audit_log(query_id);

-- Trigger to update updated_at on virtual_tables
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_virtual_tables_updated_at
    BEFORE UPDATE ON virtual_tables
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
