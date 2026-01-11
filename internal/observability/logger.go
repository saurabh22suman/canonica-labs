// Package observability provides structured logging for the canonica gateway.
// Per docs/plan.md Section 9: "Structured logging only."
//
// Every query must emit: query_id, user, tables referenced, engine selected,
// execution time, and error (if any).
package observability

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"
)

// QueryLogEntry contains all required fields for query logging.
// Per docs/plan.md Section 9 (Observability - REQUIRED).
// Per docs/phase-4-spec.md §5 (Operational Logging).
type QueryLogEntry struct {
	// QueryID is the unique identifier for this query.
	// Required: Every query must have an ID.
	QueryID string

	// User is the authenticated user who executed the query.
	// Required: Every query must be attributed to a user.
	User string

	// Role is the user's role (if applicable).
	// Phase 4: Added for authorization logging.
	Role string

	// Tables are the virtual tables referenced in the query.
	// May be empty for queries like "SELECT 1".
	Tables []string

	// AuthorizationDecision indicates if the query was authorized.
	// Phase 4: "allowed", "denied", or empty if not applicable.
	AuthorizationDecision string

	// PlannerDecision indicates the planner's routing decision.
	// Phase 4: Brief description of routing logic applied.
	PlannerDecision string

	// Engine is the execution engine selected for the query.
	// May be empty if query failed before engine selection.
	Engine string

	// ExecutionTime is how long the query took to execute.
	// Must be non-negative.
	ExecutionTime time.Duration

	// Outcome is the result status: "success", "error", "rejected".
	// Phase 4: Required for clear failure diagnosis.
	Outcome string

	// Error contains the error message if the query failed.
	// Empty string for successful queries.
	Error string

	// InvariantViolated indicates which invariant was violated (if any).
	// Phase 4: "Silent failures are forbidden."
	InvariantViolated string
}

// Validate checks that all required fields are present.
func (e *QueryLogEntry) Validate() error {
	if e.QueryID == "" {
		return fmt.Errorf("observability: query_id is required")
	}
	if e.User == "" {
		return fmt.Errorf("observability: user is required")
	}
	if e.ExecutionTime < 0 {
		return fmt.Errorf("observability: execution_time cannot be negative")
	}
	return nil
}

// QueryLogger is the interface for query logging.
type QueryLogger interface {
	// LogQuery logs a query execution event.
	// Returns an error if logging fails or the entry is invalid.
	LogQuery(ctx context.Context, entry QueryLogEntry) error

	// GetAuditSummary returns aggregated audit statistics.
	// Per phase-5-spec.md §4: "No raw data exposure"
	GetAuditSummary() *AuditSummary
}

// AuditSummary represents aggregated audit statistics.
// Per phase-5-spec.md §4: "No raw data exposure"
type AuditSummary struct {
	AcceptedCount       int                   `json:"accepted_count"`
	RejectedCount       int                   `json:"rejected_count"`
	TopRejectionReasons []RejectionReasonStat `json:"top_rejection_reasons"`
	TopQueriedTables    []TableQueryStat      `json:"top_queried_tables"`
}

// RejectionReasonStat represents rejection reason statistics.
type RejectionReasonStat struct {
	Reason string `json:"reason"`
	Count  int    `json:"count"`
}

// TableQueryStat represents table query statistics.
type TableQueryStat struct {
	Table string `json:"table"`
	Count int    `json:"count"`
}

// jsonLogOutput is the structured format for JSON logs.
// Per phase-4-spec.md §5: Every request MUST log these fields.
type jsonLogOutput struct {
	Timestamp             string   `json:"timestamp"`
	Level                 string   `json:"level"`
	QueryID               string   `json:"query_id"`
	User                  string   `json:"user"`
	Role                  string   `json:"role,omitempty"`
	Tables                []string `json:"tables"`
	AuthorizationDecision string   `json:"authorization_decision,omitempty"`
	PlannerDecision       string   `json:"planner_decision,omitempty"`
	Engine                string   `json:"engine"`
	ExecutionTimeMs       int64    `json:"execution_time_ms"`
	Outcome               string   `json:"outcome,omitempty"`
	Error                 string   `json:"error,omitempty"`
	InvariantViolated     string   `json:"invariant_violated,omitempty"`
}

// JSONLogger implements QueryLogger with JSON output.
type JSONLogger struct {
	writer  io.Writer
	entries []QueryLogEntry // Track entries for audit summary
	mu      sync.RWMutex
}

// NewJSONLogger creates a new JSON logger writing to the given writer.
func NewJSONLogger(w io.Writer) *JSONLogger {
	return &JSONLogger{
		writer:  w,
		entries: make([]QueryLogEntry, 0),
	}
}

// LogQuery logs a query execution event as JSON.
func (l *JSONLogger) LogQuery(ctx context.Context, entry QueryLogEntry) error {
	// Check context first
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("observability: context error: %w", err)
	}

	// Validate entry
	if err := entry.Validate(); err != nil {
		return err
	}

	// Determine log level
	level := "info"
	if entry.Error != "" {
		level = "error"
	}

	// Build output
	output := jsonLogOutput{
		Timestamp:             time.Now().UTC().Format(time.RFC3339),
		Level:                 level,
		QueryID:               entry.QueryID,
		User:                  entry.User,
		Role:                  entry.Role,
		Tables:                entry.Tables,
		AuthorizationDecision: entry.AuthorizationDecision,
		PlannerDecision:       entry.PlannerDecision,
		Engine:                entry.Engine,
		ExecutionTimeMs:       entry.ExecutionTime.Milliseconds(),
		Outcome:               entry.Outcome,
		Error:                 entry.Error,
		InvariantViolated:     entry.InvariantViolated,
	}

	// Ensure tables is never nil in JSON
	if output.Tables == nil {
		output.Tables = []string{}
	}

	// Encode as JSON
	data, err := json.Marshal(output)
	if err != nil {
		return fmt.Errorf("observability: failed to marshal log: %w", err)
	}

	// Write to output
	_, err = l.writer.Write(data)
	if err != nil {
		return fmt.Errorf("observability: failed to write log: %w", err)
	}

	// Track entry for audit summary
	l.mu.Lock()
	l.entries = append(l.entries, entry)
	l.mu.Unlock()

	return nil
}

// GetAuditSummary returns aggregated audit statistics.
// Per phase-5-spec.md §4: "No raw data exposure"
func (l *JSONLogger) GetAuditSummary() *AuditSummary {
	l.mu.RLock()
	defer l.mu.RUnlock()

	summary := &AuditSummary{
		TopRejectionReasons: []RejectionReasonStat{},
		TopQueriedTables:    []TableQueryStat{},
	}

	rejectionReasons := make(map[string]int)
	tableCounts := make(map[string]int)

	for _, entry := range l.entries {
		if entry.Error == "" {
			summary.AcceptedCount++
		} else {
			summary.RejectedCount++
			rejectionReasons[entry.Error]++
		}

		for _, table := range entry.Tables {
			tableCounts[table]++
		}
	}

	// Build top rejection reasons
	for reason, count := range rejectionReasons {
		summary.TopRejectionReasons = append(summary.TopRejectionReasons, RejectionReasonStat{
			Reason: reason,
			Count:  count,
		})
	}
	sort.Slice(summary.TopRejectionReasons, func(i, j int) bool {
		return summary.TopRejectionReasons[i].Count > summary.TopRejectionReasons[j].Count
	})
	if len(summary.TopRejectionReasons) > 5 {
		summary.TopRejectionReasons = summary.TopRejectionReasons[:5]
	}

	// Build top queried tables
	for table, count := range tableCounts {
		summary.TopQueriedTables = append(summary.TopQueriedTables, TableQueryStat{
			Table: table,
			Count: count,
		})
	}
	sort.Slice(summary.TopQueriedTables, func(i, j int) bool {
		return summary.TopQueriedTables[i].Count > summary.TopQueriedTables[j].Count
	})
	if len(summary.TopQueriedTables) > 5 {
		summary.TopQueriedTables = summary.TopQueriedTables[:5]
	}

	return summary
}

// NoopLogger is a logger that discards all logs.
// Useful for testing or when logging is disabled.
type NoopLogger struct{}

// NewNoopLogger creates a new no-op logger.
func NewNoopLogger() *NoopLogger {
	return &NoopLogger{}
}

// LogQuery does nothing and always succeeds.
func (l *NoopLogger) LogQuery(ctx context.Context, entry QueryLogEntry) error {
	return nil
}

// GetAuditSummary returns an empty summary for the no-op logger.
func (l *NoopLogger) GetAuditSummary() *AuditSummary {
	return &AuditSummary{
		TopRejectionReasons: []RejectionReasonStat{},
		TopQueriedTables:    []TableQueryStat{},
	}
}

// PersistentLogger implements QueryLogger with PostgreSQL persistence.
// Per T030: Audit logs must be persisted to PostgreSQL.
// Per phase-4-spec.md §5: Every request MUST log these fields.
type PersistentLogger struct {
	db     *sql.DB
	mu     sync.RWMutex
	writer io.Writer // optional: also write to stdout for debugging
}

// NewPersistentLogger creates a logger that persists audit entries to PostgreSQL.
// Per T030: Audit logs must survive gateway restart.
func NewPersistentLogger(db *sql.DB) (*PersistentLogger, error) {
	if db == nil {
		return nil, fmt.Errorf("observability: database connection is required for persistent logging")
	}
	return &PersistentLogger{
		db: db,
	}, nil
}

// NewPersistentLoggerWithWriter creates a logger that persists to both DB and a writer.
func NewPersistentLoggerWithWriter(db *sql.DB, w io.Writer) (*PersistentLogger, error) {
	if db == nil {
		return nil, fmt.Errorf("observability: database connection is required for persistent logging")
	}
	return &PersistentLogger{
		db:     db,
		writer: w,
	}, nil
}

// LogQuery persists a query log entry to PostgreSQL.
// Per T030: Audit entries must be written to audit_logs table.
func (l *PersistentLogger) LogQuery(ctx context.Context, entry QueryLogEntry) error {
	// Check context first
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("observability: context error: %w", err)
	}

	// Validate entry
	if err := entry.Validate(); err != nil {
		return err
	}

	// Convert tables to JSON
	tablesJSON, err := json.Marshal(entry.Tables)
	if err != nil {
		tablesJSON = []byte("[]")
	}

	// Insert into audit_logs
	query := `
		INSERT INTO audit_logs (
			query_id, user_id, role, tables_json, auth_decision,
			planner_decision, engine, execution_time_ms, outcome,
			error_message, invariant_violated
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`

	_, err = l.db.ExecContext(ctx, query,
		entry.QueryID,
		entry.User,
		nullableString(entry.Role),
		tablesJSON,
		nullableString(entry.AuthorizationDecision),
		nullableString(entry.PlannerDecision),
		nullableString(entry.Engine),
		entry.ExecutionTime.Milliseconds(),
		nullableString(entry.Outcome),
		nullableString(entry.Error),
		nullableString(entry.InvariantViolated),
	)
	if err != nil {
		return fmt.Errorf("observability: failed to persist audit log: %w", err)
	}

	// Also write to optional writer (for debugging)
	if l.writer != nil {
		level := "info"
		if entry.Error != "" {
			level = "error"
		}
		output := jsonLogOutput{
			Timestamp:             time.Now().UTC().Format(time.RFC3339),
			Level:                 level,
			QueryID:               entry.QueryID,
			User:                  entry.User,
			Role:                  entry.Role,
			Tables:                entry.Tables,
			AuthorizationDecision: entry.AuthorizationDecision,
			PlannerDecision:       entry.PlannerDecision,
			Engine:                entry.Engine,
			ExecutionTimeMs:       entry.ExecutionTime.Milliseconds(),
			Outcome:               entry.Outcome,
			Error:                 entry.Error,
			InvariantViolated:     entry.InvariantViolated,
		}
		if data, err := json.Marshal(output); err == nil {
			l.writer.Write(data)
			l.writer.Write([]byte("\n"))
		}
	}

	return nil
}

// GetAuditSummary returns aggregated audit statistics from the database.
// Per phase-5-spec.md §4: "No raw data exposure"
// Per T030: Summary must be retrieved from persisted data.
func (l *PersistentLogger) GetAuditSummary() *AuditSummary {
	summary := &AuditSummary{
		TopRejectionReasons: []RejectionReasonStat{},
		TopQueriedTables:    []TableQueryStat{},
	}

	ctx := context.Background()

	// Get accepted count
	row := l.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM audit_logs WHERE error_message IS NULL OR error_message = ''
	`)
	row.Scan(&summary.AcceptedCount)

	// Get rejected count
	row = l.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM audit_logs WHERE error_message IS NOT NULL AND error_message != ''
	`)
	row.Scan(&summary.RejectedCount)

	// Get top rejection reasons
	rows, err := l.db.QueryContext(ctx, `
		SELECT error_message, COUNT(*) as cnt
		FROM audit_logs
		WHERE error_message IS NOT NULL AND error_message != ''
		GROUP BY error_message
		ORDER BY cnt DESC
		LIMIT 5
	`)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var reason string
			var count int
			if rows.Scan(&reason, &count) == nil {
				summary.TopRejectionReasons = append(summary.TopRejectionReasons, RejectionReasonStat{
					Reason: reason,
					Count:  count,
				})
			}
		}
	}

	// Get top queried tables
	rows, err = l.db.QueryContext(ctx, `
		SELECT table_name, COUNT(*) as cnt
		FROM audit_logs, jsonb_array_elements_text(tables_json) as table_name
		GROUP BY table_name
		ORDER BY cnt DESC
		LIMIT 5
	`)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var table string
			var count int
			if rows.Scan(&table, &count) == nil {
				summary.TopQueriedTables = append(summary.TopQueriedTables, TableQueryStat{
					Table: table,
					Count: count,
				})
			}
		}
	}

	return summary
}

// nullableString converts empty strings to nil for SQL NULL.
func nullableString(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
