// Package redflag contains tests that verify the system correctly rejects unsafe operations.
// Per docs/test.md: "Red-Flag tests must fail before implementation and prove unsafe behavior is blocked."
package redflag

import (
	"context"
	"testing"
	"time"

	"github.com/canonica-labs/canonica/internal/adapters"
	"github.com/canonica-labs/canonica/internal/adapters/spark"
	"github.com/canonica-labs/canonica/internal/planner"
	"github.com/canonica-labs/canonica/internal/sql"
)

// TestSpark_RejectsNilExecutionPlan verifies the adapter rejects nil execution plans.
// Red-Flag: Nil plans must not cause panics or undefined behavior.
func TestSpark_RejectsNilExecutionPlan(t *testing.T) {
	adapter := spark.NewAdapter(spark.AdapterConfig{
		Host: "localhost",
		Port: 10000,
	})
	defer adapter.Close()

	_, err := adapter.Execute(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error for nil execution plan, got nil")
	}
}

// TestSpark_RejectsNilLogicalPlan verifies the adapter rejects plans with nil logical plan.
// Red-Flag: Malformed plans must be rejected explicitly.
func TestSpark_RejectsNilLogicalPlan(t *testing.T) {
	adapter := spark.NewAdapter(spark.AdapterConfig{
		Host: "localhost",
		Port: 10000,
	})
	defer adapter.Close()

	plan := &planner.ExecutionPlan{
		LogicalPlan: nil,
		Engine:      "spark",
	}

	_, err := adapter.Execute(context.Background(), plan)
	if err == nil {
		t.Fatal("expected error for nil logical plan, got nil")
	}
}

// TestSpark_RejectsEmptySQL verifies the adapter rejects empty SQL queries.
// Red-Flag: Empty queries must be rejected, not silently succeed.
func TestSpark_RejectsEmptySQL(t *testing.T) {
	adapter := spark.NewAdapter(spark.AdapterConfig{
		Host: "localhost",
		Port: 10000,
	})
	defer adapter.Close()

	plan := &planner.ExecutionPlan{
		LogicalPlan: &sql.LogicalPlan{
			RawSQL: "",
		},
		Engine: "spark",
	}

	_, err := adapter.Execute(context.Background(), plan)
	if err == nil {
		t.Fatal("expected error for empty SQL, got nil")
	}
}

// TestSpark_RejectsContextCancellation verifies the adapter respects context cancellation.
// Red-Flag: Cancelled contexts must not proceed with execution.
func TestSpark_RejectsContextCancellation(t *testing.T) {
	adapter := spark.NewAdapter(spark.AdapterConfig{
		Host: "localhost",
		Port: 10000,
	})
	defer adapter.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	plan := &planner.ExecutionPlan{
		LogicalPlan: &sql.LogicalPlan{
			RawSQL: "SELECT 1",
		},
		Engine: "spark",
	}

	_, err := adapter.Execute(ctx, plan)
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}

// TestSpark_RejectsContextTimeout verifies the adapter respects context timeout.
// Red-Flag: Timed-out contexts must not proceed with execution.
func TestSpark_RejectsContextTimeout(t *testing.T) {
	adapter := spark.NewAdapter(spark.AdapterConfig{
		Host: "localhost",
		Port: 10000,
	})
	defer adapter.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	time.Sleep(2 * time.Millisecond) // Ensure timeout

	plan := &planner.ExecutionPlan{
		LogicalPlan: &sql.LogicalPlan{
			RawSQL: "SELECT 1",
		},
		Engine: "spark",
	}

	_, err := adapter.Execute(ctx, plan)
	if err == nil {
		t.Fatal("expected error for timed-out context, got nil")
	}
}

// TestSpark_RejectsQueryOnClosedAdapter verifies that executing on a closed adapter fails.
// Red-Flag: Closed adapters must not accept queries.
func TestSpark_RejectsQueryOnClosedAdapter(t *testing.T) {
	adapter := spark.NewAdapter(spark.AdapterConfig{
		Host: "localhost",
		Port: 10000,
	})
	adapter.Close()

	plan := &planner.ExecutionPlan{
		LogicalPlan: &sql.LogicalPlan{
			RawSQL: "SELECT 1",
		},
		Engine: "spark",
	}

	_, err := adapter.Execute(context.Background(), plan)
	if err == nil {
		t.Fatal("expected error for query on closed adapter, got nil")
	}
}

// TestSpark_RejectsPingOnClosedAdapter verifies that pinging a closed adapter fails.
// Red-Flag: Closed adapters must not report as healthy.
func TestSpark_RejectsPingOnClosedAdapter(t *testing.T) {
	adapter := spark.NewAdapter(spark.AdapterConfig{
		Host: "localhost",
		Port: 10000,
	})
	adapter.Close()

	err := adapter.Ping(context.Background())
	if err == nil {
		t.Fatal("expected error for ping on closed adapter, got nil")
	}
}

// TestSpark_RejectsInvalidHost verifies the adapter rejects invalid host configuration.
// Red-Flag: Invalid configuration must be caught early.
func TestSpark_RejectsInvalidHost(t *testing.T) {
	adapter := spark.NewAdapter(spark.AdapterConfig{
		Host: "", // Empty host
		Port: 10000,
	})
	defer adapter.Close()

	plan := &planner.ExecutionPlan{
		LogicalPlan: &sql.LogicalPlan{
			RawSQL: "SELECT 1",
		},
		Engine: "spark",
	}

	_, err := adapter.Execute(context.Background(), plan)
	if err == nil {
		t.Fatal("expected error for invalid host, got nil")
	}
}

// TestSpark_ImplementsEngineAdapter verifies the adapter implements the interface.
// This is a compile-time check embedded in a test.
func TestSpark_ImplementsEngineAdapter(t *testing.T) {
	var _ adapters.EngineAdapter = (*spark.Adapter)(nil)
}

// Phase 6 Red-Flag Tests: Health Check
// Per phase-6-spec.md: Verify error handling for unreachable servers

// TestSpark_CheckHealthRejectsClosedAdapter verifies CheckHealth fails on closed adapter.
// Red-Flag: Closed adapters must not report as healthy.
func TestSpark_CheckHealthRejectsClosedAdapter(t *testing.T) {
	adapter := spark.NewAdapter(spark.AdapterConfig{
		Host: "localhost",
		Port: 10000,
	})
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

// TestSpark_CheckHealthRejectsEmptyHost verifies CheckHealth fails when host is not configured.
// Red-Flag: Missing configuration must be caught during health check.
func TestSpark_CheckHealthRejectsEmptyHost(t *testing.T) {
	adapter := spark.NewAdapter(spark.AdapterConfig{
		Host: "", // Empty host
		Port: 10000,
	})
	defer adapter.Close()

	err := adapter.CheckHealth(context.Background())
	if err == nil {
		t.Fatal("expected error for CheckHealth with empty host, got nil")
	}
}

// TestSpark_CheckHealthRejectsUnreachableServer verifies CheckHealth fails for unreachable servers.
// Red-Flag: Unreachable servers must not be reported as healthy.
// Per phase-6-spec.md: "Health check must fail if server is unreachable"
func TestSpark_CheckHealthRejectsUnreachableServer(t *testing.T) {
	// Configure with port that is very unlikely to be in use
	adapter := spark.NewAdapter(spark.AdapterConfig{
		Host:              "localhost",
		Port:              59998, // Unlikely to be in use
		ConnectionTimeout: 100 * time.Millisecond,
	})
	defer adapter.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := adapter.CheckHealth(ctx)
	if err == nil {
		t.Fatal("expected error for CheckHealth on unreachable server, got nil")
	}
}

// TestSpark_CheckHealthExplicitTimeout verifies CheckHealth uses configured timeout.
// Red-Flag: Health checks must not hang indefinitely.
func TestSpark_CheckHealthExplicitTimeout(t *testing.T) {
	adapter := spark.NewAdapter(spark.AdapterConfig{
		Host:              "10.255.255.1", // Non-routable IP - will timeout
		Port:              10000,
		ConnectionTimeout: 50 * time.Millisecond,
	})
	defer adapter.Close()

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := adapter.CheckHealth(ctx)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error for unreachable server, got nil")
	}

	// Verify it didn't take too long (should respect the short timeout)
	if elapsed > 2*time.Second {
		t.Fatalf("CheckHealth took too long: %v (expected < 2s)", elapsed)
	}
}
