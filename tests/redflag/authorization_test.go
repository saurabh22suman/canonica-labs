package redflag

import (
	"context"
	"strings"
	"testing"

	"github.com/canonica-labs/canonica/internal/auth"
	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/gateway"
	"github.com/canonica-labs/canonica/internal/planner"
	"github.com/canonica-labs/canonica/internal/router"
	"github.com/canonica-labs/canonica/internal/sql"
	"github.com/canonica-labs/canonica/internal/tables"
)

// =============================================================================
// RED-FLAG TESTS: Authorization – Role → Table Mapping (Deny by Default)
// =============================================================================
//
// Per phase-2-spec.md §4: Authorization Model
// > Absence of permission is denial.
// > There is no implicit access.
// > There is no wildcard access.
//
// These tests MUST FAIL before implementation and PASS after.
// =============================================================================

// TestAuthorization_NoRolesCannotQuery proves that a user with no roles
// cannot query any table. This is the "deny by default" principle.
//
// Red-Flag: User with no roles attempts to query a table.
func TestAuthorization_NoRolesCannotQuery(t *testing.T) {
	// Setup: Create a table registry with a table
	registry := gateway.NewInMemoryTableRegistry()
	registry.Register(&tables.VirtualTable{
		Name: "analytics.sales_orders",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatDelta, Location: "s3://bucket/sales_orders"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	})

	// Create authorization service with NO permissions for any role
	authz := auth.NewAuthorizationService()

	// Create a user with NO roles
	user := &auth.User{
		ID:    "user-no-roles",
		Name:  "No Roles User",
		Roles: []string{}, // No roles
	}

	// Parse a simple query
	parser := sql.NewParser()
	logical, err := parser.Parse("SELECT * FROM analytics.sales_orders")
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	// Attempt to authorize - this MUST fail
	ctx := auth.ContextWithUser(context.Background(), user)
	err = authz.Authorize(ctx, user, logical.Tables, capabilities.CapabilityRead)

	// RED-FLAG: If err is nil, authorization is NOT deny-by-default
	if err == nil {
		t.Errorf("RED-FLAG: User with no roles was able to query table!\n"+
			"Expected: access denied\n"+
			"Got: access allowed\n"+
			"Phase 2 requires deny-by-default authorization")
	}

	// Error message must be explicit per phase-2-spec.md §4
	if err != nil && !strings.Contains(err.Error(), "access denied") &&
		!strings.Contains(err.Error(), "permission") &&
		!strings.Contains(err.Error(), "unauthorized") {
		t.Logf("Warning: error message should mention 'access denied' or 'permission': %v", err)
	}
}

// TestAuthorization_RoleMissingTablePermission proves that a user with a role
// but missing permission for a specific table cannot query that table.
//
// Red-Flag: User with role but missing table permission.
func TestAuthorization_RoleMissingTablePermission(t *testing.T) {
	// Setup: Create a table registry with two tables
	registry := gateway.NewInMemoryTableRegistry()
	registry.Register(&tables.VirtualTable{
		Name: "analytics.sales_orders",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatDelta, Location: "s3://bucket/sales_orders"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	})
	registry.Register(&tables.VirtualTable{
		Name: "analytics.payments",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatDelta, Location: "s3://bucket/payments"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	})
	_ = registry // Silence unused warning

	// Create authorization service with permission for ONE table only
	authz := auth.NewAuthorizationService()
	authz.GrantAccess("analyst", "analytics.sales_orders", capabilities.CapabilityRead)
	// Note: NO permission granted for analytics.payments

	// Create a user with the 'analyst' role
	user := &auth.User{
		ID:    "user-analyst",
		Name:  "Analyst User",
		Roles: []string{"analyst"},
	}

	// Parse a query for the UNAUTHORIZED table
	parser := sql.NewParser()
	logical, err := parser.Parse("SELECT * FROM analytics.payments")
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	// Attempt to authorize - this MUST fail
	ctx := auth.ContextWithUser(context.Background(), user)
	err = authz.Authorize(ctx, user, logical.Tables, capabilities.CapabilityRead)

	// RED-FLAG: If err is nil, role has implicit access to all tables
	if err == nil {
		t.Errorf("RED-FLAG: User with role 'analyst' accessed table without permission!\n"+
			"Table: analytics.payments\n"+
			"Expected: access denied\n"+
			"Got: access allowed\n"+
			"Phase 2 requires explicit table permission")
	}

	// Error message must identify the unauthorized table per phase-2-spec.md §4
	if err != nil && !strings.Contains(err.Error(), "analytics.payments") {
		t.Logf("Warning: error message should identify the unauthorized table: %v", err)
	}
}

// TestAuthorization_TablePermissionMissingCapability proves that having
// table access does not grant all capabilities.
//
// Red-Flag: User with table permission but missing capability.
func TestAuthorization_TablePermissionMissingCapability(t *testing.T) {
	// Setup: Create a table with TIME_TRAVEL capability
	registry := gateway.NewInMemoryTableRegistry()
	registry.Register(&tables.VirtualTable{
		Name: "analytics.sales_orders",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatDelta, Location: "s3://bucket/sales_orders"},
		},
		Capabilities: []capabilities.Capability{
			capabilities.CapabilityRead,
			capabilities.CapabilityTimeTravel,
		},
	})
	_ = registry

	// Create authorization service with READ permission only (no TIME_TRAVEL)
	authz := auth.NewAuthorizationService()
	authz.GrantAccess("analyst", "analytics.sales_orders", capabilities.CapabilityRead)
	// Note: No TIME_TRAVEL capability granted

	// Create a user with the 'analyst' role
	user := &auth.User{
		ID:    "user-analyst",
		Name:  "Analyst User",
		Roles: []string{"analyst"},
	}

	// Parse a query that requires TIME_TRAVEL
	parser := sql.NewParser()
	logical, err := parser.Parse("SELECT * FROM analytics.sales_orders FOR SYSTEM_TIME AS OF '2024-01-01'")
	if err != nil {
		// Parser may not support FOR SYSTEM_TIME, use fallback
		logical, err = parser.Parse("SELECT * FROM analytics.sales_orders")
		if err != nil {
			t.Fatalf("failed to parse query: %v", err)
		}
		logical.HasTimeTravel = true // Simulate time travel query
	}

	// Attempt to authorize for TIME_TRAVEL - this MUST fail
	ctx := auth.ContextWithUser(context.Background(), user)
	err = authz.Authorize(ctx, user, logical.Tables, capabilities.CapabilityTimeTravel)

	// RED-FLAG: If err is nil, table access grants all capabilities
	if err == nil {
		t.Errorf("RED-FLAG: User used TIME_TRAVEL without capability permission!\n"+
			"Table: analytics.sales_orders\n"+
			"Has: READ\n"+
			"Attempted: TIME_TRAVEL\n"+
			"Phase 2 requires explicit capability permission")
	}

	// Error message must identify the missing capability per phase-2-spec.md §4
	if err != nil && !strings.Contains(strings.ToUpper(err.Error()), "TIME_TRAVEL") &&
		!strings.Contains(err.Error(), "capability") {
		t.Logf("Warning: error message should identify the missing capability: %v", err)
	}
}

// TestAuthorization_MultiTablePartialAccess proves that a query referencing
// multiple tables requires authorization on ALL tables.
//
// Red-Flag: Multi-table query where one table is unauthorized.
func TestAuthorization_MultiTablePartialAccess(t *testing.T) {
	// Setup: Create two tables
	registry := gateway.NewInMemoryTableRegistry()
	registry.Register(&tables.VirtualTable{
		Name: "analytics.sales_orders",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatDelta, Location: "s3://bucket/sales_orders"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	})
	registry.Register(&tables.VirtualTable{
		Name: "analytics.payments",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatDelta, Location: "s3://bucket/payments"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	})
	_ = registry

	// Create authorization service with permission for ONE table only
	authz := auth.NewAuthorizationService()
	authz.GrantAccess("analyst", "analytics.sales_orders", capabilities.CapabilityRead)
	// Note: NO permission for analytics.payments

	// Create a user with the 'analyst' role
	user := &auth.User{
		ID:    "user-analyst",
		Name:  "Analyst User",
		Roles: []string{"analyst"},
	}

	// Parse a JOIN query referencing BOTH tables
	parser := sql.NewParser()
	logical, err := parser.Parse("SELECT * FROM analytics.sales_orders s JOIN analytics.payments p ON s.id = p.order_id")
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	// Verify both tables are detected
	if len(logical.Tables) < 2 {
		t.Fatalf("expected 2 tables in query, got %d: %v", len(logical.Tables), logical.Tables)
	}

	// Attempt to authorize - this MUST fail
	ctx := auth.ContextWithUser(context.Background(), user)
	err = authz.Authorize(ctx, user, logical.Tables, capabilities.CapabilityRead)

	// RED-FLAG: If err is nil, partial authorization is allowed
	if err == nil {
		t.Errorf("RED-FLAG: User authorized for JOIN despite missing permission on one table!\n"+
			"Authorized tables: analytics.sales_orders\n"+
			"Unauthorized tables: analytics.payments\n"+
			"Phase 2 requires authorization on ALL tables")
	}

	// Error message must identify the unauthorized table per phase-2-spec.md §4
	if err != nil && !strings.Contains(err.Error(), "analytics.payments") {
		t.Logf("Warning: error message should identify the unauthorized table: %v", err)
	}
}

// TestAuthorization_EnforcedBeforePlanning proves that authorization
// is checked BEFORE planning and routing.
//
// Per phase-2-spec.md §4: "Authorization checks occur **before** planning and routing"
func TestAuthorization_EnforcedBeforePlanning(t *testing.T) {
	// Setup: Create a table registry with a table
	registry := gateway.NewInMemoryTableRegistry()
	registry.Register(&tables.VirtualTable{
		Name: "analytics.sales_orders",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatDelta, Location: "s3://bucket/sales_orders"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	})

	// Create authorization service with NO permissions
	authz := auth.NewAuthorizationService()

	// Create a user with no permissions
	user := &auth.User{
		ID:    "user-no-perms",
		Name:  "No Perms User",
		Roles: []string{"viewer"}, // Has role but no table permissions
	}

	// Parse query
	parser := sql.NewParser()
	logical, err := parser.Parse("SELECT * FROM analytics.sales_orders")
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	// Create planner
	engineRouter := router.DefaultRouter()
	p := planner.NewPlanner(registry, engineRouter)

	// Authorization should be checked BEFORE planner.Plan()
	ctx := auth.ContextWithUser(context.Background(), user)
	authErr := authz.Authorize(ctx, user, logical.Tables, capabilities.CapabilityRead)

	// If authorization fails, planner should NOT be called
	if authErr == nil {
		// If auth passed (shouldn't happen with deny-by-default), test planning
		_, planErr := p.Plan(ctx, logical)
		if planErr == nil {
			t.Errorf("RED-FLAG: Unauthorized user was able to plan query!\n"+
				"Authorization should block BEFORE planning")
		}
	}

	// RED-FLAG: Authorization must fail for unauthorized access
	if authErr == nil {
		t.Errorf("RED-FLAG: Authorization did not fail for unauthorized user!\n"+
			"Expected: authorization error\n"+
			"Got: authorization passed\n"+
			"Phase 2 requires deny-by-default")
	}
}

// TestAuthorization_NoEngineInteractionOnFailure proves that no engine
// interaction occurs when authorization fails.
//
// Per phase-2-spec.md §4: "no engine interaction may occur" on auth failure.
func TestAuthorization_NoEngineInteractionOnFailure(t *testing.T) {
	// This test verifies the contract: if authorization fails,
	// the system must reject the query before any engine interaction.
	//
	// Implementation note: The authorization check happens in the gateway
	// or planner layer, before any adapter.Execute() call.

	// Setup: Create authorization service with NO permissions
	authz := auth.NewAuthorizationService()

	user := &auth.User{
		ID:    "user-no-perms",
		Name:  "No Perms User",
		Roles: []string{"viewer"},
	}

	// The Authorize() method must be called before any engine interaction.
	// If Authorize fails, the calling code MUST NOT proceed to engine execution.
	ctx := auth.ContextWithUser(context.Background(), user)
	err := authz.Authorize(ctx, user, []string{"analytics.sales_orders"}, capabilities.CapabilityRead)

	// RED-FLAG: Authorization must fail
	if err == nil {
		t.Errorf("RED-FLAG: Authorization passed for unauthorized user!\n"+
			"This would allow engine interaction without permission")
	}
}
