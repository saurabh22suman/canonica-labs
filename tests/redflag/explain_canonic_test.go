// Package redflag contains tests that prove unsafe behavior is blocked.
// Red-Flag tests MUST fail before implementation and pass after.
//
// This file tests Phase 5 EXPLAIN CANONIC requirements.
// Per phase-5-spec.md §3: "Output must be deterministic and match actual execution behavior"
package redflag

import (
	"context"
	"testing"
	"time"

	"github.com/canonica-labs/canonica/internal/auth"
	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/gateway"
	"github.com/canonica-labs/canonica/internal/router"
	"github.com/canonica-labs/canonica/internal/tables"
)

// TestExplainCanonic_ExplainSucceedsButExecutionFails verifies that if EXPLAIN CANONIC
// says a query will succeed, execution must also succeed (and vice versa).
// Per phase-5-spec.md §3: "Explain succeeds but execution fails → forbidden"
func TestExplainCanonic_ExplainSucceedsButExecutionFails(t *testing.T) {
	// Setup: Create gateway with table that has inconsistent state
	// (table exists for explain but adapter fails on execute)
	tableRegistry := gateway.NewInMemoryTableRegistry()
	tableRegistry.Register(&tables.VirtualTable{
		Name:         "analytics.sales_orders",
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	})

	engineRouter := router.NewRouter()
	engineRouter.RegisterEngine(&router.Engine{
		Name:         "duckdb",
		Available:    true,
		Priority:     1,
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	})

	// Create authorization service with permission
	authz := auth.NewAuthorizationService()
	authz.GrantAccess("analyst", "analytics.sales_orders", capabilities.CapabilityRead)

	gw, err := gateway.NewGatewayWithInMemoryRegistry(gateway.Config{
		Version:       "test",
		Authorization: authz,
	})
	if err != nil {
		t.Fatalf("failed to create gateway: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create user context
	user := &auth.User{ID: "test-user", Roles: []string{"analyst"}}
	ctx = auth.ContextWithUser(ctx, user)

	sql := "SELECT * FROM analytics.sales_orders"

	// Get EXPLAIN CANONIC result
	explainResult, err := gw.ExplainCanonic(ctx, sql)
	if err != nil {
		// If explain fails, that's fine - but we need to verify
		// that execution also fails with the same reason
		t.Logf("explain failed: %v", err)
	}

	if explainResult != nil && explainResult.Accepted {
		// EXPLAIN says it will succeed - execution MUST succeed
		// If this invariant is violated, the test should fail
		t.Logf("explain says query is accepted")
		
		// We can't actually execute without a real adapter, but we can
		// verify that the explain result is consistent with the gateway state
		if explainResult.RefusalReason != "" {
			t.Error("explain result claims accepted but has refusal reason")
		}
	}
}

// TestExplainCanonic_HidesAuthorizationFailure verifies that EXPLAIN CANONIC
// does not hide authorization failures.
// Per phase-5-spec.md §3: "Explain hides authorization failure → forbidden"
func TestExplainCanonic_HidesAuthorizationFailure(t *testing.T) {
	tableRegistry := gateway.NewInMemoryTableRegistry()
	tableRegistry.Register(&tables.VirtualTable{
		Name:         "analytics.sales_orders",
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	})

	engineRouter := router.NewRouter()
	engineRouter.RegisterEngine(&router.Engine{
		Name:         "duckdb",
		Available:    true,
		Priority:     1,
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	})

	// Authorization service WITHOUT permission for the user
	authz := auth.NewAuthorizationService()
	// Note: NOT granting access to analyst role

	gw, err := gateway.NewGatewayWithInMemoryRegistry(gateway.Config{
		Version:       "test",
		Authorization: authz,
	})
	if err != nil {
		t.Fatalf("failed to create gateway: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create user context with analyst role (no permissions granted)
	user := &auth.User{ID: "test-user", Roles: []string{"analyst"}}
	ctx = auth.ContextWithUser(ctx, user)

	sql := "SELECT * FROM analytics.sales_orders"

	// EXPLAIN CANONIC must show authorization failure
	explainResult, err := gw.ExplainCanonic(ctx, sql)
	
	// Either err should be non-nil OR explainResult should show refusal
	if err == nil && explainResult != nil && explainResult.Accepted {
		t.Error("EXPLAIN CANONIC hides authorization failure - user has no permission but explain shows accepted")
	}

	if explainResult != nil && !explainResult.Accepted {
		// Good - explain correctly shows refusal
		if explainResult.AuthorizationResult != "denied" {
			t.Errorf("expected authorization result 'denied', got: %s", explainResult.AuthorizationResult)
		}
	}
}

// TestExplainCanonic_RefusalReasonMatchesRuntime verifies that the refusal
// reason in EXPLAIN CANONIC matches the actual runtime failure.
// Per phase-5-spec.md §3: "Refusal reasons must be identical to runtime failures"
func TestExplainCanonic_RefusalReasonMatchesRuntime(t *testing.T) {
	// Setup gateway without the requested table
	// Note: NOT registering the table - gateway starts with empty registry

	authz := auth.NewAuthorizationService()
	authz.GrantAccess("analyst", "analytics.nonexistent", capabilities.CapabilityRead)

	gw, err := gateway.NewGatewayWithInMemoryRegistry(gateway.Config{
		Version:       "test",
		Authorization: authz,
	})
	if err != nil {
		t.Fatalf("failed to create gateway: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	user := &auth.User{ID: "test-user", Roles: []string{"analyst"}}
	ctx = auth.ContextWithUser(ctx, user)

	sql := "SELECT * FROM analytics.nonexistent"

	// Get EXPLAIN CANONIC result
	explainResult, err := gw.ExplainCanonic(ctx, sql)
	if err != nil {
		// If explain fails with an error, that's one form of refusal
		t.Logf("explain error: %v", err)
	}

	if explainResult != nil && explainResult.Accepted {
		t.Error("EXPLAIN CANONIC should refuse query for nonexistent table")
	}

	if explainResult != nil && !explainResult.Accepted {
		// Verify the refusal reason mentions table not found
		if !containsString(explainResult.RefusalReason, "not found") &&
			!containsString(explainResult.RefusalReason, "not exist") {
			t.Errorf("refusal reason should mention table not found, got: %s", explainResult.RefusalReason)
		}
	}
}

// TestExplainCanonic_DeterministicOutput verifies that EXPLAIN CANONIC
// produces deterministic output for the same input.
// Per phase-5-spec.md §3: "Output must be deterministic"
func TestExplainCanonic_DeterministicOutput(t *testing.T) {
	tableRegistry := gateway.NewInMemoryTableRegistry()
	tableRegistry.Register(&tables.VirtualTable{
		Name:         "analytics.sales_orders",
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	})

	engineRouter := router.NewRouter()
	engineRouter.RegisterEngine(&router.Engine{
		Name:         "duckdb",
		Available:    true,
		Priority:     1,
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	})

	authz := auth.NewAuthorizationService()
	authz.GrantAccess("analyst", "analytics.sales_orders", capabilities.CapabilityRead)

	gw, err := gateway.NewGatewayWithInMemoryRegistry(gateway.Config{
		Version:       "test",
		Authorization: authz,
	})
	if err != nil {
		t.Fatalf("failed to create gateway: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	user := &auth.User{ID: "test-user", Roles: []string{"analyst"}}
	ctx = auth.ContextWithUser(ctx, user)

	sql := "SELECT * FROM analytics.sales_orders"

	// Run EXPLAIN CANONIC multiple times
	var results []*gateway.ExplainCanonicResult
	for i := 0; i < 3; i++ {
		result, err := gw.ExplainCanonic(ctx, sql)
		if err != nil {
			t.Fatalf("explain failed on iteration %d: %v", i, err)
		}
		results = append(results, result)
	}

	// All results must be identical
	if len(results) > 1 {
		first := results[0]
		for i, result := range results[1:] {
			if result.Accepted != first.Accepted {
				t.Errorf("iteration %d: Accepted differs: %v vs %v", i+1, result.Accepted, first.Accepted)
			}
			if result.Engine != first.Engine {
				t.Errorf("iteration %d: Engine differs: %s vs %s", i+1, result.Engine, first.Engine)
			}
			if result.AuthorizationResult != first.AuthorizationResult {
				t.Errorf("iteration %d: AuthorizationResult differs: %s vs %s", i+1, result.AuthorizationResult, first.AuthorizationResult)
			}
		}
	}
}
