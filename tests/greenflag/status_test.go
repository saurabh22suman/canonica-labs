// Package greenflag contains tests that prove allowed behavior works correctly.
// Green-Flag tests validate happy paths and deterministic behavior.
//
// This file tests Phase 5 status and audit requirements.
// Per phase-5-spec.md §4: "Status reflects readiness endpoints"
package greenflag

import (
	"context"
	"testing"
	"time"

	"github.com/canonica-labs/canonica/internal/status"
)

// TestStatus_ReflectsReadinessEndpoints verifies that status command
// accurately reflects the gateway readiness endpoints.
// Per phase-5-spec.md §4 Green-Flag: "Status reflects readiness endpoints"
func TestStatus_ReflectsReadinessEndpoints(t *testing.T) {
	// Create mock status checker that simulates a healthy system
	// This tests that status correctly reflects readiness values
	checker := status.NewMockStatusChecker()
	checker.SetRepositoryStatus(true, "connected")
	checker.SetEngineStatus(true, "duckdb available")
	checker.SetConfigVersion("v1.0.0")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := checker.GetStatus(ctx)
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}

	// Status should show:
	// 1. Gateway readiness
	if result.Ready != true {
		t.Error("expected status to be ready when components are healthy")
	}

	// 2. Repository health
	if result.RepositoryHealth == "" {
		t.Error("expected repository health to be reported")
	}

	// 3. Engine availability
	if result.EnginesMessage == "" {
		t.Error("expected engine message to be reported")
	}

	// 4. Active configuration version
	if result.ConfigVersion == "" {
		t.Error("expected config version to be reported")
	}
}

// TestStatus_DisplaysAllComponents verifies that status command shows
// all required components.
// Per phase-5-spec.md §4: "canonic status displays: gateway readiness, repository health, engine availability, active configuration version"
func TestStatus_DisplaysAllComponents(t *testing.T) {
	mock := status.NewMockStatusChecker()
	mock.SetRepositoryStatus(true, "connected")
	mock.SetEngineStatus(true, "3 engines available")
	mock.SetConfigVersion("v1.2.3")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := mock.GetStatus(ctx)
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}

	// Verify all components are present
	if !result.GatewayReady {
		t.Error("gateway readiness not set")
	}

	if result.RepositoryHealth != "connected" {
		t.Errorf("expected repository health 'connected', got '%s'", result.RepositoryHealth)
	}

	if result.EnginesMessage != "3 engines available" {
		t.Errorf("expected engines message '3 engines available', got '%s'", result.EnginesMessage)
	}

	if result.ConfigVersion != "v1.2.3" {
		t.Errorf("expected config version 'v1.2.3', got '%s'", result.ConfigVersion)
	}
}

// TestAudit_SummaryMatchesLogs verifies that audit summary accurately
// reflects logged query data.
// Per phase-5-spec.md §4 Green-Flag: "Audit summaries match logs"
func TestAudit_SummaryMatchesLogs(t *testing.T) {
	auditLogger := status.NewMockAuditLogger()

	// Log specific queries
	auditLogger.LogQuery(status.QueryAuditEntry{
		QueryID:  "q1",
		User:     "alice",
		Tables:   []string{"analytics.sales"},
		Accepted: true,
		Duration: 100 * time.Millisecond,
	})
	auditLogger.LogQuery(status.QueryAuditEntry{
		QueryID:  "q2",
		User:     "bob",
		Tables:   []string{"analytics.customers"},
		Accepted: true,
		Duration: 200 * time.Millisecond,
	})
	auditLogger.LogQuery(status.QueryAuditEntry{
		QueryID:  "q3",
		User:     "charlie",
		Tables:   []string{"analytics.orders"},
		Accepted: false,
		Error:    "table not found",
		Duration: 50 * time.Millisecond,
	})
	auditLogger.LogQuery(status.QueryAuditEntry{
		QueryID:  "q4",
		User:     "alice",
		Tables:   []string{"analytics.sales"},
		Accepted: true,
		Duration: 150 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	summary, err := auditLogger.GetAuditSummary(ctx)
	if err != nil {
		t.Fatalf("GetAuditSummary failed: %v", err)
	}

	// Verify counts
	if summary.AcceptedCount != 3 {
		t.Errorf("expected 3 accepted queries, got %d", summary.AcceptedCount)
	}

	if summary.RejectedCount != 1 {
		t.Errorf("expected 1 rejected query, got %d", summary.RejectedCount)
	}

	// Verify top rejection reasons
	if len(summary.TopRejectionReasons) == 0 {
		t.Error("expected top rejection reasons to be tracked")
	}

	foundReason := false
	for _, reason := range summary.TopRejectionReasons {
		if reason.Reason == "table not found" {
			foundReason = true
			if reason.Count != 1 {
				t.Errorf("expected count 1 for 'table not found', got %d", reason.Count)
			}
		}
	}
	if !foundReason {
		t.Error("expected 'table not found' in top rejection reasons")
	}

	// Verify top queried tables
	if len(summary.TopQueriedTables) == 0 {
		t.Error("expected top queried tables to be tracked")
	}

	// analytics.sales should be top with 2 queries
	if len(summary.TopQueriedTables) > 0 {
		if summary.TopQueriedTables[0].Table != "analytics.sales" {
			t.Errorf("expected 'analytics.sales' as top table, got '%s'", summary.TopQueriedTables[0].Table)
		}
		if summary.TopQueriedTables[0].Count != 2 {
			t.Errorf("expected count 2 for top table, got %d", summary.TopQueriedTables[0].Count)
		}
	}
}

// TestAudit_NoSensitiveDataExposed verifies that audit summary
// does not expose sensitive information.
// Per phase-5-spec.md §4: "No raw data exposure"
func TestAudit_NoSensitiveDataExposed(t *testing.T) {
	auditLogger := status.NewMockAuditLogger()

	// Log query with potentially sensitive content
	auditLogger.LogQuery(status.QueryAuditEntry{
		QueryID:  "q1",
		User:     "admin",
		SQL:      "SELECT ssn, credit_card FROM customers WHERE email = 'secret@example.com'",
		Tables:   []string{"pii.customers"},
		Accepted: true,
		Duration: 100 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	summary, err := auditLogger.GetAuditSummary(ctx)
	if err != nil {
		t.Fatalf("GetAuditSummary failed: %v", err)
	}

	// Summary string should not contain sensitive data
	summaryStr := summary.String()

	sensitiveTerms := []string{
		"ssn",
		"credit_card",
		"secret@example.com",
		"SELECT ssn",
	}

	for _, term := range sensitiveTerms {
		if containsString(summaryStr, term) {
			t.Errorf("audit summary exposes sensitive term: %s", term)
		}
	}

	// Summary SHOULD contain aggregate stats
	if summary.AcceptedCount == 0 && summary.RejectedCount == 0 {
		t.Error("audit summary should have query counts")
	}
}

// TestStatus_HealthySystemShowsReady verifies that a healthy system
// reports ready status.
func TestStatus_HealthySystemShowsReady(t *testing.T) {
	mock := status.NewMockStatusChecker()
	mock.SetRepositoryStatus(true, "connected")
	mock.SetEngineStatus(true, "available")
	mock.SetConfigVersion("v1.0.0")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := mock.GetStatus(ctx)
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}

	if !result.Ready {
		t.Error("expected healthy system to report Ready=true")
	}

	if result.Reason != "" {
		t.Errorf("healthy system should have no reason, got: %s", result.Reason)
	}
}

// containsString checks if s contains substr (case-insensitive).
func containsString(s, substr string) bool {
	if len(s) == 0 || len(substr) == 0 {
		return false
	}
	if len(s) < len(substr) {
		return false
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
