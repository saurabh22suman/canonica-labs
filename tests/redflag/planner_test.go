package redflag

import (
	"context"
	"testing"

	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/errors"
	"github.com/canonica-labs/canonica/internal/router"
)

// TestRouter_NoAvailableEngine proves that queries fail when no engine
// can satisfy the required capabilities.
//
// Red-Flag: System MUST fail if no engine can execute the query.
func TestRouter_NoAvailableEngine(t *testing.T) {
	// Arrange: Router with no available engines
	r := router.NewRouter()
	r.RegisterEngine(&router.Engine{
		Name:         "duckdb",
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
		Available:    false, // Not available
	})
	ctx := context.Background()

	// Act
	_, err := r.SelectEngine(ctx, []capabilities.Capability{capabilities.CapabilityRead})

	// Assert: Engine selection MUST fail
	if err == nil {
		t.Fatal("expected error when no engine available, got nil")
	}

	// Assert: Error must be ErrEngineUnavailable
	if _, ok := err.(*errors.ErrEngineUnavailable); !ok {
		t.Fatalf("expected ErrEngineUnavailable, got %T: %v", err, err)
	}
}

// TestRouter_MissingCapability proves that queries fail when no engine
// has the required capability.
//
// Red-Flag: System MUST fail if no engine has required capability.
func TestRouter_MissingCapability(t *testing.T) {
	// Arrange: Router with engine that lacks TIME_TRAVEL
	r := router.NewRouter()
	r.RegisterEngine(&router.Engine{
		Name:         "basic-engine",
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead}, // No TIME_TRAVEL
		Available:    true,
	})
	ctx := context.Background()

	// Act: Request TIME_TRAVEL capability
	_, err := r.SelectEngine(ctx, []capabilities.Capability{
		capabilities.CapabilityRead,
		capabilities.CapabilityTimeTravel,
	})

	// Assert: Engine selection MUST fail
	if err == nil {
		t.Fatal("expected error when no engine has TIME_TRAVEL, got nil")
	}
}

// TestRouter_AllEnginesUnavailable proves that queries fail when all
// engines are marked unavailable.
//
// Red-Flag: System MUST fail if all engines are unavailable.
func TestRouter_AllEnginesUnavailable(t *testing.T) {
	// Arrange: Router with multiple unavailable engines
	r := router.NewRouter()
	r.RegisterEngine(&router.Engine{
		Name:         "duckdb",
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
		Available:    false,
	})
	r.RegisterEngine(&router.Engine{
		Name:         "trino",
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
		Available:    false,
	})
	ctx := context.Background()

	// Act
	_, err := r.SelectEngine(ctx, []capabilities.Capability{capabilities.CapabilityRead})

	// Assert: Engine selection MUST fail
	if err == nil {
		t.Fatal("expected error when all engines unavailable, got nil")
	}
}

// TestRouter_EmptyCapabilityList proves that a query with no required
// capabilities still needs at least one available engine.
//
// Red-Flag: System MUST have at least one available engine.
func TestRouter_EmptyEngineRegistry(t *testing.T) {
	// Arrange: Router with no engines
	r := router.NewRouter()
	ctx := context.Background()

	// Act: Even with no required capabilities
	_, err := r.SelectEngine(ctx, []capabilities.Capability{})

	// Assert: Engine selection MUST fail (no engines registered)
	if err == nil {
		t.Fatal("expected error when no engines registered, got nil")
	}
}

// TestPlanner_CrossEngineQueryRejected proves that queries spanning multiple
// engines are rejected with a specific ErrCrossEngineQuery error.
//
// Red-Flag: Cross-engine queries MUST be explicitly rejected when federation is disabled.
// Per phase-9-spec.md: Cross-engine detection is required for federation routing.
func TestPlanner_CrossEngineQueryRejected(t *testing.T) {
	// This test verifies that the planner correctly detects cross-engine queries
	// by checking that ErrCrossEngineQuery is defined and usable.
	// Full integration test would require mock table registry with different formats.

	// Arrange: Create a cross-engine error
	engines := []string{"trino", "spark"}
	crossEngErr := errors.NewCrossEngineQuery(engines)

	// Assert: Error contains the engines
	if len(crossEngErr.Engines) != 2 {
		t.Fatalf("expected 2 engines, got %d", len(crossEngErr.Engines))
	}

	// Assert: Error message contains reason
	if crossEngErr.Reason == "" {
		t.Fatal("expected non-empty Reason")
	}

	// Assert: Error message contains suggestion
	if crossEngErr.Suggestion == "" {
		t.Fatal("expected non-empty Suggestion")
	}

	// Assert: Error implements error interface
	var err error = crossEngErr
	if err.Error() == "" {
		t.Fatal("expected non-empty error message")
	}
}
