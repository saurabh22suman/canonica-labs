// Package greenflag contains tests that verify the system correctly performs allowed operations.
// Per docs/test.md: "Green-Flag tests demonstrate allowed behavior and must be deterministic."
package greenflag

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/canonica-labs/canonica/internal/observability"
)

// TestObservability_LogValidQuery verifies successful query logging.
// Green-Flag: Valid queries with all fields must be logged.
func TestObservability_LogValidQuery(t *testing.T) {
	var buf bytes.Buffer
	logger := observability.NewJSONLogger(&buf)

	entry := observability.QueryLogEntry{
		QueryID:       "query-abc-123",
		User:          "analyst@example.com",
		Tables:        []string{"sales.orders"},
		Engine:        "duckdb",
		ExecutionTime: 150 * time.Millisecond,
	}

	err := logger.LogQuery(context.Background(), entry)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if buf.Len() == 0 {
		t.Fatal("expected log output, got empty")
	}
}

// TestObservability_LogQueryWithError verifies error logging.
// Green-Flag: Query errors must be captured in logs.
func TestObservability_LogQueryWithError(t *testing.T) {
	var buf bytes.Buffer
	logger := observability.NewJSONLogger(&buf)

	entry := observability.QueryLogEntry{
		QueryID:       "query-err-456",
		User:          "user@example.com",
		Tables:        []string{"forbidden.table"},
		Engine:        "",
		ExecutionTime: 5 * time.Millisecond,
		Error:         "READ_ONLY constraint violated",
	}

	err := logger.LogQuery(context.Background(), entry)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if parsed["error"] != "READ_ONLY constraint violated" {
		t.Errorf("expected error field, got %v", parsed["error"])
	}
}

// TestObservability_LogMultipleTables verifies multiple table references.
// Green-Flag: Queries referencing multiple tables must log all of them.
func TestObservability_LogMultipleTables(t *testing.T) {
	var buf bytes.Buffer
	logger := observability.NewJSONLogger(&buf)

	entry := observability.QueryLogEntry{
		QueryID:       "query-multi-789",
		User:          "user@example.com",
		Tables:        []string{"orders", "customers", "products"},
		Engine:        "trino",
		ExecutionTime: 500 * time.Millisecond,
	}

	err := logger.LogQuery(context.Background(), entry)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	tables, ok := parsed["tables"].([]interface{})
	if !ok {
		t.Fatalf("tables field not an array: %v", parsed["tables"])
	}

	if len(tables) != 3 {
		t.Errorf("expected 3 tables, got %d", len(tables))
	}
}

// TestObservability_ExecutionTimeInMilliseconds verifies time format.
// Green-Flag: Execution time must be logged in milliseconds for consistency.
func TestObservability_ExecutionTimeInMilliseconds(t *testing.T) {
	var buf bytes.Buffer
	logger := observability.NewJSONLogger(&buf)

	entry := observability.QueryLogEntry{
		QueryID:       "query-time-001",
		User:          "user@example.com",
		Tables:        []string{"test"},
		Engine:        "duckdb",
		ExecutionTime: 1500 * time.Millisecond, // 1.5 seconds
	}

	err := logger.LogQuery(context.Background(), entry)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	// Should be 1500 ms
	execTime, ok := parsed["execution_time_ms"].(float64)
	if !ok {
		t.Fatalf("execution_time_ms not a number: %v", parsed["execution_time_ms"])
	}

	if execTime != 1500 {
		t.Errorf("expected 1500 ms, got %v", execTime)
	}
}

// TestObservability_TimestampIncluded verifies logs include timestamp.
// Green-Flag: Every log must have a timestamp for ordering.
func TestObservability_TimestampIncluded(t *testing.T) {
	var buf bytes.Buffer
	logger := observability.NewJSONLogger(&buf)

	entry := observability.QueryLogEntry{
		QueryID:       "query-ts-002",
		User:          "user@example.com",
		Tables:        []string{"test"},
		Engine:        "duckdb",
		ExecutionTime: 10 * time.Millisecond,
	}

	err := logger.LogQuery(context.Background(), entry)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if _, ok := parsed["timestamp"]; !ok {
		t.Error("timestamp field missing from log output")
	}
}

// TestObservability_EmptyTablesAllowed verifies queries with no tables are allowed.
// Green-Flag: Some queries (e.g., SELECT 1) reference no tables.
func TestObservability_EmptyTablesAllowed(t *testing.T) {
	var buf bytes.Buffer
	logger := observability.NewJSONLogger(&buf)

	entry := observability.QueryLogEntry{
		QueryID:       "query-notables-003",
		User:          "user@example.com",
		Tables:        []string{}, // No tables
		Engine:        "duckdb",
		ExecutionTime: 1 * time.Millisecond,
	}

	err := logger.LogQuery(context.Background(), entry)
	if err != nil {
		t.Fatalf("unexpected error for empty tables: %v", err)
	}
}

// TestObservability_ZeroExecutionTimeAllowed verifies zero time is valid.
// Green-Flag: Very fast queries may report 0ms.
func TestObservability_ZeroExecutionTimeAllowed(t *testing.T) {
	var buf bytes.Buffer
	logger := observability.NewJSONLogger(&buf)

	entry := observability.QueryLogEntry{
		QueryID:       "query-zero-004",
		User:          "user@example.com",
		Tables:        []string{"fast_table"},
		Engine:        "duckdb",
		ExecutionTime: 0,
	}

	err := logger.LogQuery(context.Background(), entry)
	if err != nil {
		t.Fatalf("unexpected error for zero execution time: %v", err)
	}
}

// TestObservability_LoggerInterface verifies interface compliance.
// Green-Flag: Logger must implement the QueryLogger interface.
func TestObservability_LoggerInterface(t *testing.T) {
	var _ observability.QueryLogger = observability.NewJSONLogger(&bytes.Buffer{})
}

// TestObservability_NoopLoggerDoesNotPanic verifies noop logger is safe.
// Green-Flag: NoopLogger must be safe to use everywhere.
func TestObservability_NoopLoggerDoesNotPanic(t *testing.T) {
	logger := observability.NewNoopLogger()

	entry := observability.QueryLogEntry{
		QueryID:       "query-noop-005",
		User:          "user@example.com",
		Tables:        []string{"test"},
		Engine:        "duckdb",
		ExecutionTime: 100 * time.Millisecond,
	}

	err := logger.LogQuery(context.Background(), entry)
	if err != nil {
		t.Fatalf("noop logger should never error: %v", err)
	}
}

// TestObservability_LogLevel verifies log level is included.
// Green-Flag: Log level helps filtering and alerting.
func TestObservability_LogLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := observability.NewJSONLogger(&buf)

	entry := observability.QueryLogEntry{
		QueryID:       "query-level-006",
		User:          "user@example.com",
		Tables:        []string{"test"},
		Engine:        "duckdb",
		ExecutionTime: 10 * time.Millisecond,
	}

	err := logger.LogQuery(context.Background(), entry)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, "level") {
		t.Error("log level field missing from output")
	}
}
