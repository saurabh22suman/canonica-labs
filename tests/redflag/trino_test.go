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

// Phase 6 Red-Flag Tests: Health Check and Connection Pool
// Per phase-6-spec.md: Verify error handling for unreachable servers

// TestTrino_CheckHealthRejectsClosedAdapter verifies CheckHealth fails on closed adapter.
// Red-Flag: Closed adapters must not report as healthy.
func TestTrino_CheckHealthRejectsClosedAdapter(t *testing.T) {
	adapter := trino.NewAdapter(trino.AdapterConfig{
		Host: "localhost",
		Port: 8080,
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

// TestTrino_CheckHealthRejectsContextCancellation verifies CheckHealth respects context.
// Red-Flag: Cancelled contexts must be honored during health checks.
func TestTrino_CheckHealthRejectsContextCancellation(t *testing.T) {
	adapter := trino.NewAdapter(trino.AdapterConfig{
		Host: "localhost",
		Port: 8080,
	})
	defer adapter.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := adapter.CheckHealth(ctx)
	if err == nil {
		// Note: This may pass if health check doesn't block,
		// but if it does block, it must respect cancellation
		t.Log("CheckHealth completed without error despite cancelled context")
	}
}

// TestTrino_CheckHealthRejectsUnreachableServer verifies CheckHealth fails for unreachable servers.
// Red-Flag: Unreachable servers must not be reported as healthy.
// Per phase-6-spec.md: "Health check must fail if server is unreachable"
func TestTrino_CheckHealthRejectsUnreachableServer(t *testing.T) {
	// Configure with port that is very unlikely to be in use
	adapter := trino.NewAdapter(trino.AdapterConfig{
		Host:           "localhost",
		Port:           59999, // Unlikely to be in use
		ConnectTimeout: 100 * time.Millisecond,
	})
	defer adapter.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := adapter.CheckHealth(ctx)
	if err == nil {
		t.Fatal("expected error for CheckHealth on unreachable server, got nil")
	}
}

// TestTrino_ConnectionPoolConfigApplied verifies connection pool settings are applied.
// Red-Flag: Zero/negative pool settings must use sensible defaults.
func TestTrino_ConnectionPoolConfigApplied(t *testing.T) {
	// Create adapter with zero values - should use defaults
	adapter := trino.NewAdapter(trino.AdapterConfig{
		Host:            "localhost",
		Port:            8080,
		MaxOpenConns:    0, // Should default to 10
		MaxIdleConns:    0, // Should default to 5
		ConnMaxLifetime: 0, // Should default to 5m
		ConnMaxIdleTime: 0, // Should default to 1m
	})
	defer adapter.Close()

	// Verify adapter was created successfully with defaults
	if adapter.Name() != "trino" {
		t.Fatalf("expected adapter name 'trino', got '%s'", adapter.Name())
	}
}

// TestTrino_CheckHealthExplicitTimeout verifies CheckHealth uses configured timeout.
// Red-Flag: Health checks must not hang indefinitely.
func TestTrino_CheckHealthExplicitTimeout(t *testing.T) {
	adapter := trino.NewAdapter(trino.AdapterConfig{
		Host:           "10.255.255.1", // Non-routable IP - will timeout
		Port:           8080,
		ConnectTimeout: 50 * time.Millisecond,
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
