// Package redflag contains tests that verify the system correctly rejects unsafe operations.
// Per docs/test.md: "Red-Flag tests must fail before implementation and prove unsafe behavior is blocked."
package redflag

import (
	"context"
	"testing"
	"time"

	"github.com/canonica-labs/canonica/internal/adapters"
	"github.com/canonica-labs/canonica/internal/adapters/trino"
	"github.com/canonica-labs/canonica/internal/planner"
	"github.com/canonica-labs/canonica/internal/sql"
)

// TestTrino_RejectsNilExecutionPlan verifies the adapter rejects nil execution plans.
// Red-Flag: Nil plans must not cause panics or undefined behavior.
func TestTrino_RejectsNilExecutionPlan(t *testing.T) {
	adapter := trino.NewAdapter(trino.AdapterConfig{
		Host: "localhost",
		Port: 8080,
	})
	defer adapter.Close()

	_, err := adapter.Execute(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error for nil execution plan, got nil")
	}
}

// TestTrino_RejectsNilLogicalPlan verifies the adapter rejects plans with nil logical plan.
// Red-Flag: Malformed plans must be rejected explicitly.
func TestTrino_RejectsNilLogicalPlan(t *testing.T) {
	adapter := trino.NewAdapter(trino.AdapterConfig{
		Host: "localhost",
		Port: 8080,
	})
	defer adapter.Close()

	plan := &planner.ExecutionPlan{
		LogicalPlan: nil,
		Engine:      "trino",
	}

	_, err := adapter.Execute(context.Background(), plan)
	if err == nil {
		t.Fatal("expected error for nil logical plan, got nil")
	}
}

// TestTrino_RejectsEmptySQL verifies the adapter rejects empty SQL queries.
// Red-Flag: Empty queries must be rejected, not silently succeed.
func TestTrino_RejectsEmptySQL(t *testing.T) {
	adapter := trino.NewAdapter(trino.AdapterConfig{
		Host: "localhost",
		Port: 8080,
	})
	defer adapter.Close()

	plan := &planner.ExecutionPlan{
		LogicalPlan: &sql.LogicalPlan{
			RawSQL: "",
		},
		Engine: "trino",
	}

	_, err := adapter.Execute(context.Background(), plan)
	if err == nil {
		t.Fatal("expected error for empty SQL, got nil")
	}
}

// TestTrino_RejectsContextCancellation verifies the adapter respects context cancellation.
// Red-Flag: Cancelled contexts must not proceed with execution.
func TestTrino_RejectsContextCancellation(t *testing.T) {
	adapter := trino.NewAdapter(trino.AdapterConfig{
		Host: "localhost",
		Port: 8080,
	})
	defer adapter.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	plan := &planner.ExecutionPlan{
		LogicalPlan: &sql.LogicalPlan{
			RawSQL: "SELECT 1",
		},
		Engine: "trino",
	}

	_, err := adapter.Execute(ctx, plan)
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}

// TestTrino_RejectsContextTimeout verifies the adapter respects context timeout.
// Red-Flag: Timed-out contexts must not proceed with execution.
func TestTrino_RejectsContextTimeout(t *testing.T) {
	adapter := trino.NewAdapter(trino.AdapterConfig{
		Host: "localhost",
		Port: 8080,
	})
	defer adapter.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	time.Sleep(2 * time.Millisecond) // Ensure timeout

	plan := &planner.ExecutionPlan{
		LogicalPlan: &sql.LogicalPlan{
			RawSQL: "SELECT 1",
		},
		Engine: "trino",
	}

	_, err := adapter.Execute(ctx, plan)
	if err == nil {
		t.Fatal("expected error for timed-out context, got nil")
	}
}

// TestTrino_RejectsQueryOnClosedAdapter verifies that executing on a closed adapter fails.
// Red-Flag: Closed adapters must not accept queries.
func TestTrino_RejectsQueryOnClosedAdapter(t *testing.T) {
	adapter := trino.NewAdapter(trino.AdapterConfig{
		Host: "localhost",
		Port: 8080,
	})
	adapter.Close()

	plan := &planner.ExecutionPlan{
		LogicalPlan: &sql.LogicalPlan{
			RawSQL: "SELECT 1",
		},
		Engine: "trino",
	}

	_, err := adapter.Execute(context.Background(), plan)
	if err == nil {
		t.Fatal("expected error for query on closed adapter, got nil")
	}
}

// TestTrino_RejectsPingOnClosedAdapter verifies that pinging a closed adapter fails.
// Red-Flag: Closed adapters must not report as healthy.
func TestTrino_RejectsPingOnClosedAdapter(t *testing.T) {
	adapter := trino.NewAdapter(trino.AdapterConfig{
		Host: "localhost",
		Port: 8080,
	})
	adapter.Close()

	err := adapter.Ping(context.Background())
	if err == nil {
		t.Fatal("expected error for ping on closed adapter, got nil")
	}
}

// TestTrino_RejectsInvalidHost verifies the adapter rejects invalid host configuration.
// Red-Flag: Invalid configuration must be caught early.
func TestTrino_RejectsInvalidHost(t *testing.T) {
	adapter := trino.NewAdapter(trino.AdapterConfig{
		Host: "", // Empty host
		Port: 8080,
	})
	defer adapter.Close()

	plan := &planner.ExecutionPlan{
		LogicalPlan: &sql.LogicalPlan{
			RawSQL: "SELECT 1",
		},
		Engine: "trino",
	}

	_, err := adapter.Execute(context.Background(), plan)
	if err == nil {
		t.Fatal("expected error for invalid host, got nil")
	}
}

// TestTrino_ImplementsEngineAdapter verifies the adapter implements the interface.
// This is a compile-time check embedded in a test.
func TestTrino_ImplementsEngineAdapter(t *testing.T) {
	var _ adapters.EngineAdapter = (*trino.Adapter)(nil)
}
