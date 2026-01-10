// Package redflag contains tests that verify the system correctly rejects unsafe operations.
// Per docs/test.md: "Red-Flag tests must fail before implementation and prove unsafe behavior is blocked."
package redflag

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/canonica-labs/canonica/internal/observability"
)

// TestObservability_RejectsEmptyQueryID verifies that logs without query_id are rejected.
// Red-Flag: Per plan.md, every query must emit query_id.
func TestObservability_RejectsEmptyQueryID(t *testing.T) {
	var buf bytes.Buffer
	logger := observability.NewJSONLogger(&buf)

	entry := observability.QueryLogEntry{
		QueryID: "", // Empty - should be rejected
		User:    "test-user",
	}

	err := logger.LogQuery(context.Background(), entry)
	if err == nil {
		t.Fatal("expected error for empty query_id, got nil")
	}
}

// TestObservability_RejectsEmptyUser verifies that logs without user are rejected.
// Red-Flag: Per plan.md, every query must emit user.
func TestObservability_RejectsEmptyUser(t *testing.T) {
	var buf bytes.Buffer
	logger := observability.NewJSONLogger(&buf)

	entry := observability.QueryLogEntry{
		QueryID: "query-123",
		User:    "", // Empty - should be rejected
	}

	err := logger.LogQuery(context.Background(), entry)
	if err == nil {
		t.Fatal("expected error for empty user, got nil")
	}
}

// TestObservability_RejectsContextCancellation verifies logging respects context.
// Red-Flag: Cancelled contexts must not proceed.
func TestObservability_RejectsContextCancellation(t *testing.T) {
	var buf bytes.Buffer
	logger := observability.NewJSONLogger(&buf)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	entry := observability.QueryLogEntry{
		QueryID: "query-123",
		User:    "test-user",
	}

	err := logger.LogQuery(ctx, entry)
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}

// TestObservability_OutputIsValidJSON verifies logs are structured JSON.
// Red-Flag: Per plan.md, "Structured logging only" - unstructured logs are forbidden.
func TestObservability_OutputIsValidJSON(t *testing.T) {
	var buf bytes.Buffer
	logger := observability.NewJSONLogger(&buf)

	entry := observability.QueryLogEntry{
		QueryID:       "query-123",
		User:          "test-user",
		Tables:        []string{"orders"},
		Engine:        "duckdb",
		ExecutionTime: 100 * time.Millisecond,
	}

	err := logger.LogQuery(context.Background(), entry)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify output is valid JSON
	output := buf.Bytes()
	if len(output) == 0 {
		t.Fatal("expected log output, got empty")
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(output, &parsed); err != nil {
		t.Fatalf("log output is not valid JSON: %v\nOutput: %s", err, string(output))
	}
}

// TestObservability_RejectsNegativeExecutionTime verifies invalid timing is rejected.
// Red-Flag: Negative execution times indicate measurement errors.
func TestObservability_RejectsNegativeExecutionTime(t *testing.T) {
	var buf bytes.Buffer
	logger := observability.NewJSONLogger(&buf)

	entry := observability.QueryLogEntry{
		QueryID:       "query-123",
		User:          "test-user",
		ExecutionTime: -1 * time.Second,
	}

	err := logger.LogQuery(context.Background(), entry)
	if err == nil {
		t.Fatal("expected error for negative execution time, got nil")
	}
}

// TestObservability_RequiredFieldsInOutput verifies all plan.md required fields are present.
// Red-Flag: Missing required fields violate observability requirements.
func TestObservability_RequiredFieldsInOutput(t *testing.T) {
	var buf bytes.Buffer
	logger := observability.NewJSONLogger(&buf)

	entry := observability.QueryLogEntry{
		QueryID:       "query-123",
		User:          "test-user",
		Tables:        []string{"orders", "customers"},
		Engine:        "duckdb",
		ExecutionTime: 50 * time.Millisecond,
		Error:         "",
	}

	err := logger.LogQuery(context.Background(), entry)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	// Per plan.md: query_id, user, tables referenced, engine selected, execution time, error
	requiredFields := []string{"query_id", "user", "tables", "engine", "execution_time_ms"}
	for _, field := range requiredFields {
		if _, ok := parsed[field]; !ok {
			t.Errorf("required field %q missing from log output", field)
		}
	}
}
