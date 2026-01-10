// Package redflag contains Red-Flag tests that prove the system correctly
// refuses unsafe, ambiguous, or unsupported behavior.
//
// Per docs/test.md: "Red-Flag tests assert that the system REFUSES unsafe, ambiguous,
// or unsupported behavior."
//
// Per docs/phase-4-spec.md §5: "Silent failures are forbidden"
// - Every request MUST log required fields
// - Failures MUST log explicit reason and invariant violated
package redflag

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/canonica-labs/canonica/internal/observability"
)

// TestLoggingRejectsEmptyQueryID tests that empty query_id is rejected.
// Per phase-4-spec.md §5: "Every request MUST log: request_id"
func TestLoggingRejectsEmptyQueryID(t *testing.T) {
	var buf bytes.Buffer
	logger := observability.NewJSONLogger(&buf)

	entry := observability.QueryLogEntry{
		QueryID:       "", // Empty - should fail
		User:          "test-user",
		ExecutionTime: 100 * time.Millisecond,
	}

	err := logger.LogQuery(context.Background(), entry)
	if err == nil {
		t.Error("Logging MUST reject entries without query_id")
	}
}

// TestLoggingRejectsEmptyUser tests that empty user is rejected.
// Per phase-4-spec.md §5: "Every request MUST log: user / role"
func TestLoggingRejectsEmptyUser(t *testing.T) {
	var buf bytes.Buffer
	logger := observability.NewJSONLogger(&buf)

	entry := observability.QueryLogEntry{
		QueryID:       "q123",
		User:          "", // Empty - should fail
		ExecutionTime: 100 * time.Millisecond,
	}

	err := logger.LogQuery(context.Background(), entry)
	if err == nil {
		t.Error("Logging MUST reject entries without user")
	}
}

// TestLoggingRejectsNegativeExecutionTime tests that negative time is rejected.
// Per phase-4-spec.md §5: "Every request MUST log: outcome"
func TestLoggingRejectsNegativeExecutionTime(t *testing.T) {
	var buf bytes.Buffer
	logger := observability.NewJSONLogger(&buf)

	entry := observability.QueryLogEntry{
		QueryID:       "q123",
		User:          "test-user",
		ExecutionTime: -1 * time.Millisecond, // Invalid
	}

	err := logger.LogQuery(context.Background(), entry)
	if err == nil {
		t.Error("Logging MUST reject entries with negative execution time")
	}
}

// TestLoggingIncludesAuthorizationDecision tests that authorization is logged.
// Per phase-4-spec.md §5: "Every request MUST log: authorization decision"
func TestLoggingIncludesAuthorizationDecision(t *testing.T) {
	var buf bytes.Buffer
	logger := observability.NewJSONLogger(&buf)

	entry := observability.QueryLogEntry{
		QueryID:               "q123",
		User:                  "test-user",
		AuthorizationDecision: "allowed",
		ExecutionTime:         100 * time.Millisecond,
	}

	err := logger.LogQuery(context.Background(), entry)
	if err != nil {
		t.Fatalf("Logging failed: %v", err)
	}

	var output map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &output); err != nil {
		t.Fatalf("Failed to parse log output: %v", err)
	}

	if output["authorization_decision"] != "allowed" {
		t.Error("Authorization decision should be logged")
	}
}

// TestLoggingIncludesInvariantViolated tests that invariant violations are logged.
// Per phase-4-spec.md §5: "Failures MUST log: invariant violated"
func TestLoggingIncludesInvariantViolated(t *testing.T) {
	var buf bytes.Buffer
	logger := observability.NewJSONLogger(&buf)

	entry := observability.QueryLogEntry{
		QueryID:           "q123",
		User:              "test-user",
		Error:             "capability check failed",
		InvariantViolated: "READ capability required",
		ExecutionTime:     100 * time.Millisecond,
	}

	err := logger.LogQuery(context.Background(), entry)
	if err != nil {
		t.Fatalf("Logging failed: %v", err)
	}

	var output map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &output); err != nil {
		t.Fatalf("Failed to parse log output: %v", err)
	}

	if output["invariant_violated"] != "READ capability required" {
		t.Error("Invariant violated should be logged for failures")
	}
}
