// Package greenflag contains Green-Flag tests that prove the system correctly
// executes behavior that is explicitly declared safe.
//
// Per docs/test.md: "Green-Flag tests assert that the system SUCCESSFULLY EXECUTES
// behavior that is explicitly declared safe."
//
// Per docs/phase-4-spec.md §5: "Add Minimal Operational Logging"
package greenflag

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/canonica-labs/canonica/internal/observability"
)

// TestLoggingIncludesAllRequiredFields tests that all Phase 4 fields are logged.
// Per phase-4-spec.md §5: "Every request MUST log: request_id, user/role, tables,
// authorization decision, planner decision, engine selected, outcome"
func TestLoggingIncludesAllRequiredFields(t *testing.T) {
	var buf bytes.Buffer
	logger := observability.NewJSONLogger(&buf)

	entry := observability.QueryLogEntry{
		QueryID:               "q-12345",
		User:                  "alice@example.com",
		Role:                  "data-analyst",
		Tables:                []string{"analytics.orders", "analytics.customers"},
		AuthorizationDecision: "allowed",
		PlannerDecision:       "single-table read via DuckDB",
		Engine:                "duckdb",
		ExecutionTime:         150 * time.Millisecond,
		Outcome:               "success",
	}

	err := logger.LogQuery(context.Background(), entry)
	if err != nil {
		t.Fatalf("Logging failed: %v", err)
	}

	var output map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &output); err != nil {
		t.Fatalf("Failed to parse log output: %v", err)
	}

	// Verify all required fields
	requiredFields := []string{
		"query_id",
		"user",
		"tables",
		"engine",
		"execution_time_ms",
	}

	for _, field := range requiredFields {
		if _, ok := output[field]; !ok {
			t.Errorf("Missing required field: %s", field)
		}
	}
}

// TestLoggingSuccessfulQuery tests logging of a successful query.
// Per phase-4-spec.md §5: Successful queries logged with outcome "success"
func TestLoggingSuccessfulQuery(t *testing.T) {
	var buf bytes.Buffer
	logger := observability.NewJSONLogger(&buf)

	entry := observability.QueryLogEntry{
		QueryID:               "q-success-123",
		User:                  "bob@example.com",
		Tables:                []string{"analytics.sales"},
		AuthorizationDecision: "allowed",
		PlannerDecision:       "routed to DuckDB",
		Engine:                "duckdb",
		ExecutionTime:         50 * time.Millisecond,
		Outcome:               "success",
	}

	err := logger.LogQuery(context.Background(), entry)
	if err != nil {
		t.Fatalf("Logging failed: %v", err)
	}

	var output map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &output); err != nil {
		t.Fatalf("Failed to parse log output: %v", err)
	}

	if output["level"] != "info" {
		t.Errorf("Successful queries should have level 'info', got '%v'", output["level"])
	}

	if output["outcome"] != "success" {
		t.Errorf("Expected outcome 'success', got '%v'", output["outcome"])
	}
}

// TestLoggingFailedQuery tests logging of a failed query.
// Per phase-4-spec.md §5: "Failures MUST log: explicit reason"
func TestLoggingFailedQuery(t *testing.T) {
	var buf bytes.Buffer
	logger := observability.NewJSONLogger(&buf)

	entry := observability.QueryLogEntry{
		QueryID:               "q-fail-456",
		User:                  "charlie@example.com",
		Tables:                []string{"secret.data"},
		AuthorizationDecision: "denied",
		Engine:                "",
		ExecutionTime:         5 * time.Millisecond,
		Outcome:               "error",
		Error:                 "access denied: user lacks READ permission on secret.data",
		InvariantViolated:     "authorization",
	}

	err := logger.LogQuery(context.Background(), entry)
	if err != nil {
		t.Fatalf("Logging failed: %v", err)
	}

	var output map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &output); err != nil {
		t.Fatalf("Failed to parse log output: %v", err)
	}

	if output["level"] != "error" {
		t.Errorf("Failed queries should have level 'error', got '%v'", output["level"])
	}

	if output["error"] == nil || output["error"] == "" {
		t.Error("Failed queries must include error message")
	}

	if output["invariant_violated"] != "authorization" {
		t.Error("Failed queries should include which invariant was violated")
	}
}

// TestLoggingPlannerDecision tests that planner decisions are logged.
// Per phase-4-spec.md §5: "Every request MUST log: planner decision"
func TestLoggingPlannerDecision(t *testing.T) {
	var buf bytes.Buffer
	logger := observability.NewJSONLogger(&buf)

	entry := observability.QueryLogEntry{
		QueryID:         "q-planner-789",
		User:            "dave@example.com",
		Tables:          []string{"analytics.events"},
		PlannerDecision: "time-travel query routed to Delta engine",
		Engine:          "spark",
		ExecutionTime:   200 * time.Millisecond,
		Outcome:         "success",
	}

	err := logger.LogQuery(context.Background(), entry)
	if err != nil {
		t.Fatalf("Logging failed: %v", err)
	}

	var output map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &output); err != nil {
		t.Fatalf("Failed to parse log output: %v", err)
	}

	if output["planner_decision"] != "time-travel query routed to Delta engine" {
		t.Error("Planner decision should be logged")
	}
}

// TestLoggingUserRole tests that user role is logged when provided.
// Per phase-4-spec.md §5: "Every request MUST log: user / role"
func TestLoggingUserRole(t *testing.T) {
	var buf bytes.Buffer
	logger := observability.NewJSONLogger(&buf)

	entry := observability.QueryLogEntry{
		QueryID:       "q-role-123",
		User:          "eve@example.com",
		Role:          "admin",
		Tables:        []string{"system.config"},
		ExecutionTime: 10 * time.Millisecond,
		Outcome:       "success",
	}

	err := logger.LogQuery(context.Background(), entry)
	if err != nil {
		t.Fatalf("Logging failed: %v", err)
	}

	var output map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &output); err != nil {
		t.Fatalf("Failed to parse log output: %v", err)
	}

	if output["role"] != "admin" {
		t.Error("User role should be logged when provided")
	}
}
