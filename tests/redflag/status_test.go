// Package redflag contains tests that prove unsafe behavior is blocked.
// Red-Flag tests MUST fail before implementation and pass after.
//
// This file tests Phase 5 status and audit requirements.
// Per phase-5-spec.md §4: "Status reports healthy when system is not ready" must fail
package redflag

import (
	"context"
	"testing"
	"time"

	"github.com/canonica-labs/canonica/internal/gateway"
	"github.com/canonica-labs/canonica/internal/status"
)

// TestStatus_ReportsHealthyWhenNotReady verifies that the status command
// does not falsely report healthy when the system is not ready.
// Per phase-5-spec.md §4: "Status reports healthy when system is not ready" is forbidden
func TestStatus_ReportsHealthyWhenNotReady(t *testing.T) {
	testCases := []struct {
		name          string
		setupMock     func() *status.MockStatusChecker
		expectHealthy bool
	}{
		{
			name: "repository unavailable",
			setupMock: func() *status.MockStatusChecker {
				m := status.NewMockStatusChecker()
				m.SetRepositoryStatus(false, "connection refused")
				m.SetEngineStatus(true, "available")
				m.SetConfigVersion("v1.0.0")
				return m
			},
			expectHealthy: false,
		},
		{
			name: "all engines unavailable",
			setupMock: func() *status.MockStatusChecker {
				m := status.NewMockStatusChecker()
				m.SetRepositoryStatus(true, "connected")
				m.SetEngineStatus(false, "no engines available")
				m.SetConfigVersion("v1.0.0")
				return m
			},
			expectHealthy: false,
		},
		{
			name: "no configuration version",
			setupMock: func() *status.MockStatusChecker {
				m := status.NewMockStatusChecker()
				m.SetRepositoryStatus(true, "connected")
				m.SetEngineStatus(true, "available")
				m.SetConfigVersion("") // No config loaded
				return m
			},
			expectHealthy: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mock := tc.setupMock()
			
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			result, err := mock.GetStatus(ctx)
			if err != nil {
				t.Fatalf("GetStatus failed: %v", err)
			}

			if result.Ready != tc.expectHealthy {
				t.Errorf("expected Ready=%v, got Ready=%v", tc.expectHealthy, result.Ready)
			}

			// When not ready, must have a reason
			if !result.Ready && result.Reason == "" {
				t.Error("status is not ready but no reason provided")
			}
		})
	}
}

// TestAudit_ExposesSensitiveData verifies that the audit summary
// does not expose sensitive data.
// Per phase-5-spec.md §4: "Audit exposes sensitive data" is forbidden
func TestAudit_ExposesSensitiveData(t *testing.T) {
	// Create mock audit logger with sensitive data
	auditLogger := status.NewMockAuditLogger()
	
	// Log some queries with sensitive data
	auditLogger.LogQuery(status.QueryAuditEntry{
		QueryID:    "q1",
		User:       "admin",
		SQL:        "SELECT password FROM users WHERE email = 'test@example.com'",
		Tables:     []string{"auth.users"},
		Engine:     "duckdb",
		Accepted:   true,
		Duration:   100 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Get audit summary
	summary, err := auditLogger.GetAuditSummary(ctx)
	if err != nil {
		t.Fatalf("GetAuditSummary failed: %v", err)
	}

	// Summary must NOT contain:
	// - Raw SQL queries
	// - User emails or passwords
	// - Full query text

	if containsString(summary.String(), "password") {
		t.Error("audit summary exposes sensitive field name 'password'")
	}

	if containsString(summary.String(), "test@example.com") {
		t.Error("audit summary exposes email address")
	}

	if containsString(summary.String(), "SELECT password") {
		t.Error("audit summary exposes raw SQL query")
	}

	// Summary SHOULD contain aggregate data
	if summary.AcceptedCount == 0 && summary.RejectedCount == 0 {
		t.Error("audit summary should have query counts")
	}
}

// TestStatus_ReflectsReadinessEndpoint verifies that CLI status
// matches the gateway /readyz endpoint.
// Per phase-5-spec.md §4: "Status reflects readiness endpoints"
func TestStatus_ReflectsReadinessEndpoint(t *testing.T) {
	// Create a gateway in test mode
	gw, err := gateway.NewGatewayWithInMemoryRegistry(gateway.Config{
		Version: "test",
	})
	if err != nil {
		t.Fatalf("failed to create gateway: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Get readiness from gateway
	gwReadiness := gw.GetReadiness(ctx)

	// Create status checker using functional adapter
	checker := status.NewFuncStatusChecker(
		func(ctx context.Context) *status.ReadinessResult {
			r := gw.GetReadiness(ctx)
			components := make(map[string]status.ComponentStatus)
			for name, comp := range r.Components {
				components[name] = status.ComponentStatus{
					Ready:   comp.Ready,
					Message: comp.Message,
				}
			}
			return &status.ReadinessResult{
				Ready:      r.Ready,
				Components: components,
			}
		},
		gw.GetVersion,
	)
	statusResult, err := checker.GetStatus(ctx)
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}

	// Status must match readiness
	if gwReadiness.Ready != statusResult.Ready {
		t.Errorf("status Ready=%v does not match readiness Ready=%v",
			statusResult.Ready, gwReadiness.Ready)
	}
}

// TestAudit_SummaryMatchesLogs verifies that audit summary
// accurately reflects the logged data.
// Per phase-5-spec.md §4: "Audit summaries match logs"
func TestAudit_SummaryMatchesLogs(t *testing.T) {
	auditLogger := status.NewMockAuditLogger()

	// Log specific number of accepted and rejected queries
	for i := 0; i < 5; i++ {
		auditLogger.LogQuery(status.QueryAuditEntry{
			QueryID:  "accepted-" + string(rune(i)),
			Accepted: true,
		})
	}
	for i := 0; i < 3; i++ {
		auditLogger.LogQuery(status.QueryAuditEntry{
			QueryID:  "rejected-" + string(rune(i)),
			Accepted: false,
			Error:    "table not found",
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	summary, err := auditLogger.GetAuditSummary(ctx)
	if err != nil {
		t.Fatalf("GetAuditSummary failed: %v", err)
	}

	// Verify counts match
	if summary.AcceptedCount != 5 {
		t.Errorf("expected 5 accepted, got %d", summary.AcceptedCount)
	}

	if summary.RejectedCount != 3 {
		t.Errorf("expected 3 rejected, got %d", summary.RejectedCount)
	}

	// Verify top rejection reasons are tracked
	if len(summary.TopRejectionReasons) == 0 {
		t.Error("expected top rejection reasons to be tracked")
	}
}
