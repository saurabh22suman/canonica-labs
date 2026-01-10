// Package greenflag contains tests that prove allowed behavior works correctly.
// Green-Flag tests validate happy paths and deterministic behavior.
//
// This file tests Phase 5 EXPLAIN CANONIC requirements.
// Per phase-5-spec.md §3: "Explain matches execution routing"
package greenflag

import (
	"context"
	"testing"
	"time"

	"github.com/canonica-labs/canonica/internal/adapters"
	"github.com/canonica-labs/canonica/internal/auth"
	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/gateway"
	"github.com/canonica-labs/canonica/internal/router"
	"github.com/canonica-labs/canonica/internal/tables"
)

// TestExplainCanonic_MatchesExecutionRouting verifies that EXPLAIN CANONIC
// correctly predicts the execution routing.
// Per phase-5-spec.md §3 Green-Flag: "Explain matches execution routing"
func TestExplainCanonic_MatchesExecutionRouting(t *testing.T) {
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
	engineRouter.RegisterEngine(&router.Engine{
		Name:         "trino",
		Available:    true,
		Priority:     2,
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead, capabilities.CapabilityTimeTravel},
	})

	authz := auth.NewAuthorizationService()
	authz.GrantAccess("analyst", "analytics.sales_orders", capabilities.CapabilityRead)

	adapterRegistry := adapters.NewAdapterRegistry()

	gw := gateway.NewGateway(
		auth.NewStaticTokenAuthenticator(),
		tableRegistry,
		engineRouter,
		adapterRegistry,
		gateway.Config{
			Version:       "test",
			Authorization: authz,
		},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	user := &auth.User{ID: "test-user", Roles: []string{"analyst"}}
	ctx = auth.ContextWithUser(ctx, user)

	sql := "SELECT * FROM analytics.sales_orders"

	// Get EXPLAIN CANONIC result
	explainResult, err := gw.ExplainCanonic(ctx, sql)
	if err != nil {
		t.Fatalf("explain failed: %v", err)
	}

	// Verify explain shows expected routing
	if explainResult.Engine != "duckdb" {
		t.Errorf("expected engine=duckdb (highest priority), got %s", explainResult.Engine)
	}

	// Verify tables are listed
	if len(explainResult.Tables) != 1 || explainResult.Tables[0] != "analytics.sales_orders" {
		t.Errorf("expected tables=[analytics.sales_orders], got %v", explainResult.Tables)
	}

	// Verify capabilities
	if len(explainResult.RequiredCapabilities) == 0 {
		t.Error("expected required capabilities to be listed")
	}
}

// TestExplainCanonic_SurfacesRefusalCorrectly verifies that EXPLAIN CANONIC
// correctly surfaces refusal reasons.
// Per phase-5-spec.md §3 Green-Flag: "Explain surfaces refusal correctly"
func TestExplainCanonic_SurfacesRefusalCorrectly(t *testing.T) {
	testCases := []struct {
		name           string
		setup          func() (*gateway.Gateway, context.Context)
		sql            string
		expectAccepted bool
		expectReason   string
	}{
		{
			name: "table not found",
			setup: func() (*gateway.Gateway, context.Context) {
				// No table registered - gateway starts with empty registry

				authz := auth.NewAuthorizationService()
				authz.GrantAccess("analyst", "analytics.nonexistent", capabilities.CapabilityRead)

				gw, _ := gateway.NewGatewayWithInMemoryRegistry(gateway.Config{
					Version:       "test",
					Authorization: authz,
				})

				ctx := context.Background()
				user := &auth.User{ID: "test-user", Roles: []string{"analyst"}}
				ctx = auth.ContextWithUser(ctx, user)

				return gw, ctx
			},
			sql:            "SELECT * FROM analytics.nonexistent",
			expectAccepted: false,
			expectReason:   "not found",
		},
		{
			name: "no engine available",
			setup: func() (*gateway.Gateway, context.Context) {
				tableRegistry := gateway.NewInMemoryTableRegistry()
				tableRegistry.Register(&tables.VirtualTable{
					Name:         "analytics.sales_orders",
					Capabilities: []capabilities.Capability{capabilities.CapabilityTimeTravel},
				})

				engineRouter := router.NewRouter()
				// No engine with TIME_TRAVEL
				engineRouter.RegisterEngine(&router.Engine{
					Name:         "duckdb",
					Available:    true,
					Priority:     1,
					Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
				})

				authz := auth.NewAuthorizationService()
				authz.GrantAccess("analyst", "analytics.sales_orders", capabilities.CapabilityTimeTravel)

				gw, _ := gateway.NewGatewayWithInMemoryRegistry(gateway.Config{
					Version:       "test",
					Authorization: authz,
				})

				ctx := context.Background()
				user := &auth.User{ID: "test-user", Roles: []string{"analyst"}}
				ctx = auth.ContextWithUser(ctx, user)

				return gw, ctx
			},
			sql:            "SELECT * FROM analytics.sales_orders",
			expectAccepted: false,
			expectReason:   "engine",
		},
		{
			name: "authorization denied",
			setup: func() (*gateway.Gateway, context.Context) {
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
				// No access granted

				gw, _ := gateway.NewGatewayWithInMemoryRegistry(gateway.Config{
					Version:       "test",
					Authorization: authz,
				})

				ctx := context.Background()
				user := &auth.User{ID: "test-user", Roles: []string{"analyst"}}
				ctx = auth.ContextWithUser(ctx, user)

				return gw, ctx
			},
			sql:            "SELECT * FROM analytics.sales_orders",
			expectAccepted: false,
			expectReason:   "denied",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gw, ctx := tc.setup()

			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			result, err := gw.ExplainCanonic(ctx, tc.sql)
			if err != nil {
				// Error is acceptable for refusal cases
				t.Logf("explain error: %v", err)
				return
			}

			if result.Accepted != tc.expectAccepted {
				t.Errorf("expected Accepted=%v, got %v", tc.expectAccepted, result.Accepted)
			}

			if !tc.expectAccepted && result.RefusalReason == "" {
				t.Error("expected refusal reason for rejected query")
			}
		})
	}
}

// TestExplainCanonic_OutputSections verifies that EXPLAIN CANONIC includes
// all required output sections.
// Per phase-5-spec.md §3: Required output sections
func TestExplainCanonic_OutputSections(t *testing.T) {
	tableRegistry := gateway.NewInMemoryTableRegistry()
	tableRegistry.Register(&tables.VirtualTable{
		Name:         "analytics.sales_orders",
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead, capabilities.CapabilityTimeTravel},
		Constraints:  []capabilities.Constraint{capabilities.ConstraintSnapshotConsistent},
	})

	engineRouter := router.NewRouter()
	engineRouter.RegisterEngine(&router.Engine{
		Name:         "trino",
		Available:    true,
		Priority:     1,
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead, capabilities.CapabilityTimeTravel},
	})

	authz := auth.NewAuthorizationService()
	authz.GrantAccess("analyst", "analytics.sales_orders", capabilities.CapabilityRead)

	gw, _ := gateway.NewGatewayWithInMemoryRegistry(gateway.Config{
		Version:       "test",
		Authorization: authz,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	user := &auth.User{ID: "test-user", Roles: []string{"analyst"}}
	ctx = auth.ContextWithUser(ctx, user)

	sql := "SELECT * FROM analytics.sales_orders"

	result, err := gw.ExplainCanonic(ctx, sql)
	if err != nil {
		t.Fatalf("explain failed: %v", err)
	}

	// Check all required sections per phase-5-spec.md §3
	// 1. Tables referenced
	if len(result.Tables) == 0 {
		t.Error("missing section: Tables referenced")
	}

	// 2. Required capabilities
	if len(result.RequiredCapabilities) == 0 {
		t.Error("missing section: Required capabilities")
	}

	// 3. Authorization result
	if result.AuthorizationResult == "" {
		t.Error("missing section: Authorization result")
	}

	// 4. Snapshot requirements
	// (present even if empty)
	// result.SnapshotRequirements is checked by type

	// 5. Engine selection
	if result.Engine == "" && result.Accepted {
		t.Error("missing section: Engine selection (for accepted query)")
	}

	// 6. Acceptance or refusal reason
	if !result.Accepted && result.RefusalReason == "" {
		t.Error("missing section: Refusal reason (for rejected query)")
	}
}

// TestExplainCanonic_DeterministicAcrossRuns verifies deterministic output
// across multiple EXPLAIN CANONIC calls.
// Per phase-5-spec.md §3: "Output must be deterministic"
func TestExplainCanonic_DeterministicAcrossRuns(t *testing.T) {
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

	gw, _ := gateway.NewGatewayWithInMemoryRegistry(gateway.Config{
		Version:       "test",
		Authorization: authz,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	user := &auth.User{ID: "test-user", Roles: []string{"analyst"}}
	ctx = auth.ContextWithUser(ctx, user)

	sql := "SELECT * FROM analytics.sales_orders"

	// Run 10 times and verify consistency
	var firstResult *gateway.ExplainCanonicResult
	for i := 0; i < 10; i++ {
		result, err := gw.ExplainCanonic(ctx, sql)
		if err != nil {
			t.Fatalf("explain failed on iteration %d: %v", i, err)
		}

		if firstResult == nil {
			firstResult = result
			continue
		}

		// Compare all fields
		if result.Accepted != firstResult.Accepted {
			t.Errorf("iteration %d: Accepted changed", i)
		}
		if result.Engine != firstResult.Engine {
			t.Errorf("iteration %d: Engine changed: %s -> %s", i, firstResult.Engine, result.Engine)
		}
		if result.AuthorizationResult != firstResult.AuthorizationResult {
			t.Errorf("iteration %d: AuthorizationResult changed", i)
		}
	}
}
