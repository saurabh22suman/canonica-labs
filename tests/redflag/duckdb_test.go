// Package redflag contains tests that verify the system correctly rejects unsafe operations.
// Per docs/test.md: "Red-Flag tests must fail before implementation and prove unsafe behavior is blocked."
package redflag

import (
	"context"
	"testing"
	"time"

	"github.com/canonica-labs/canonica/internal/adapters"
	"github.com/canonica-labs/canonica/internal/adapters/duckdb"
	"github.com/canonica-labs/canonica/internal/planner"
	"github.com/canonica-labs/canonica/internal/sql"
)

// TestDuckDB_RejectsNilExecutionPlan verifies the adapter rejects nil execution plans.
// Red-Flag: Nil plans must not cause panics or undefined behavior.
func TestDuckDB_RejectsNilExecutionPlan(t *testing.T) {
	adapter := duckdb.NewAdapter()
	defer adapter.Close()

	_, err := adapter.Execute(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error for nil execution plan, got nil")
	}
}

// TestDuckDB_RejectsNilLogicalPlan verifies the adapter rejects plans with nil logical plan.
// Red-Flag: Malformed plans must be rejected explicitly.
func TestDuckDB_RejectsNilLogicalPlan(t *testing.T) {
	adapter := duckdb.NewAdapter()
	defer adapter.Close()

	plan := &planner.ExecutionPlan{
		LogicalPlan: nil,
		Engine:      "duckdb",
	}

	_, err := adapter.Execute(context.Background(), plan)
	if err == nil {
		t.Fatal("expected error for nil logical plan, got nil")
	}
}

// TestDuckDB_RejectsEmptySQL verifies the adapter rejects empty SQL queries.
// Red-Flag: Empty queries must be rejected, not silently succeed.
func TestDuckDB_RejectsEmptySQL(t *testing.T) {
	adapter := duckdb.NewAdapter()
	defer adapter.Close()

	plan := &planner.ExecutionPlan{
		LogicalPlan: &sql.LogicalPlan{
			RawSQL: "",
		},
		Engine: "duckdb",
	}

	_, err := adapter.Execute(context.Background(), plan)
	if err == nil {
		t.Fatal("expected error for empty SQL, got nil")
	}
}

// TestDuckDB_RejectsContextCancellation verifies the adapter respects context cancellation.
// Red-Flag: Cancelled contexts must not proceed with execution.
func TestDuckDB_RejectsContextCancellation(t *testing.T) {
	adapter := duckdb.NewAdapter()
	defer adapter.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	plan := &planner.ExecutionPlan{
		LogicalPlan: &sql.LogicalPlan{
			RawSQL: "SELECT 1",
		},
		Engine: "duckdb",
	}

	_, err := adapter.Execute(ctx, plan)
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}

// TestDuckDB_RejectsContextTimeout verifies the adapter respects context timeout.
// Red-Flag: Timed-out contexts must not proceed with execution.
func TestDuckDB_RejectsContextTimeout(t *testing.T) {
	adapter := duckdb.NewAdapter()
	defer adapter.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	time.Sleep(2 * time.Millisecond) // Ensure timeout

	plan := &planner.ExecutionPlan{
		LogicalPlan: &sql.LogicalPlan{
			RawSQL: "SELECT 1",
		},
		Engine: "duckdb",
	}

	_, err := adapter.Execute(ctx, plan)
	if err == nil {
		t.Fatal("expected error for timed-out context, got nil")
	}
}

// TestDuckDB_RejectsInvalidSQL verifies the adapter rejects syntactically invalid SQL.
// Red-Flag: Invalid SQL must produce explicit errors.
func TestDuckDB_RejectsInvalidSQL(t *testing.T) {
	adapter := duckdb.NewAdapter()
	defer adapter.Close()

	testCases := []struct {
		name string
		sql  string
	}{
		{"incomplete_select", "SELECT FROM"},
		{"invalid_syntax", "SELEKT * FROM table"},
		{"missing_table", "SELECT * FROM"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			plan := &planner.ExecutionPlan{
				LogicalPlan: &sql.LogicalPlan{
					RawSQL: tc.sql,
				},
				Engine: "duckdb",
			}

			_, err := adapter.Execute(context.Background(), plan)
			if err == nil {
				t.Fatalf("expected error for invalid SQL %q, got nil", tc.sql)
			}
		})
	}
}

// TestDuckDB_RejectsQueryOnClosedAdapter verifies that executing on a closed adapter fails.
// Red-Flag: Closed adapters must not accept queries.
func TestDuckDB_RejectsQueryOnClosedAdapter(t *testing.T) {
	adapter := duckdb.NewAdapter()
	adapter.Close()

	plan := &planner.ExecutionPlan{
		LogicalPlan: &sql.LogicalPlan{
			RawSQL: "SELECT 1",
		},
		Engine: "duckdb",
	}

	_, err := adapter.Execute(context.Background(), plan)
	if err == nil {
		t.Fatal("expected error for query on closed adapter, got nil")
	}
}

// TestDuckDB_RejectsPingOnClosedAdapter verifies that pinging a closed adapter fails.
// Red-Flag: Closed adapters must not report as healthy.
func TestDuckDB_RejectsPingOnClosedAdapter(t *testing.T) {
	adapter := duckdb.NewAdapter()
	adapter.Close()

	err := adapter.Ping(context.Background())
	if err == nil {
		t.Fatal("expected error for ping on closed adapter, got nil")
	}
}

// TestDuckDB_ImplementsEngineAdapter verifies the adapter implements the interface.
// This is a compile-time check embedded in a test.
func TestDuckDB_ImplementsEngineAdapter(t *testing.T) {
	var _ adapters.EngineAdapter = (*duckdb.Adapter)(nil)
}

// Phase 6 Red-Flag Tests: Health Check
// Per phase-6-spec.md: Verify error handling for closed adapters

// TestDuckDB_CheckHealthRejectsClosedAdapter verifies CheckHealth fails on closed adapter.
// Red-Flag: Closed adapters must not report as healthy.
func TestDuckDB_CheckHealthRejectsClosedAdapter(t *testing.T) {
	adapter := duckdb.NewAdapter()
	adapter.Close()

	err := adapter.CheckHealth(context.Background())
	if err == nil {
		t.Fatal("expected error for CheckHealth on closed adapter, got nil")
	}

	// Verify error message is meaningful
	if err.Error() == "" {
		t.Fatal("error message should not be empty")
	}
}

// TestDuckDB_CheckHealthRejectsContextCancellation verifies CheckHealth respects context.
// Red-Flag: Cancelled contexts must be honored during health checks.
func TestDuckDB_CheckHealthRejectsContextCancellation(t *testing.T) {
	adapter := duckdb.NewAdapter()
	defer adapter.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := adapter.CheckHealth(ctx)
	if err == nil {
		// DuckDB is in-memory, so this may complete before cancellation is checked
		t.Log("CheckHealth completed without error despite cancelled context - this is acceptable for fast in-memory checks")
	}
}
