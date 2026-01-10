package greenflag

import (
	"context"
	"testing"

	"github.com/canonica-labs/canonica/internal/auth"
	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/gateway"
	"github.com/canonica-labs/canonica/internal/sql"
	"github.com/canonica-labs/canonica/internal/tables"
)

// =============================================================================
// GREEN-FLAG TESTS: Authorization – Role → Table Mapping
// =============================================================================
//
// Per phase-2-spec.md §4: Green-Flag Tests (Required)
// - Authorized role can query permitted table
// - Authorized role with correct capability can use AS OF
// - Multi-table query where all tables are permitted
//
// These tests verify expected behavior for VALID authorization scenarios.
// =============================================================================

// TestAuthorization_AuthorizedRoleCanQuery proves that a user with the
// correct role and table permission can query the table.
//
// Green-Flag: Authorized role can query permitted table.
func TestAuthorization_AuthorizedRoleCanQuery(t *testing.T) {
	// Setup: Create a table registry with a table
	registry := gateway.NewInMemoryTableRegistry()
	registry.Register(&tables.VirtualTable{
		Name: "analytics.sales_orders",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatDelta, Location: "s3://bucket/sales_orders"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	})
	_ = registry

	// Create authorization service with READ permission on the table
	authz := auth.NewAuthorizationService()
	authz.GrantAccess("analyst", "analytics.sales_orders", capabilities.CapabilityRead)

	// Create a user with the 'analyst' role
	user := &auth.User{
		ID:    "user-analyst",
		Name:  "Analyst User",
		Roles: []string{"analyst"},
	}

	// Parse a query for the authorized table
	parser := sql.NewParser()
	logical, err := parser.Parse("SELECT * FROM analytics.sales_orders")
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	// Attempt to authorize - this MUST succeed
	ctx := auth.ContextWithUser(context.Background(), user)
	err = authz.Authorize(ctx, user, logical.Tables, capabilities.CapabilityRead)

	// GREEN-FLAG: Authorization must pass for authorized user
	if err != nil {
		t.Errorf("GREEN-FLAG VIOLATION: Authorized user was denied access!\n"+
			"User: %s\n"+
			"Role: analyst\n"+
			"Table: analytics.sales_orders\n"+
			"Capability: READ\n"+
			"Error: %v", user.ID, err)
	}
}

// TestAuthorization_AuthorizedRoleCanUseTimeTravel proves that a user with
// TIME_TRAVEL capability can use AS OF queries.
//
// Green-Flag: Authorized role with correct capability can use AS OF.
func TestAuthorization_AuthorizedRoleCanUseTimeTravel(t *testing.T) {
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

	// Create authorization service with TIME_TRAVEL permission
	authz := auth.NewAuthorizationService()
	authz.GrantAccess("analyst", "analytics.sales_orders", capabilities.CapabilityRead)
	authz.GrantAccess("analyst", "analytics.sales_orders", capabilities.CapabilityTimeTravel)

	// Create a user with the 'analyst' role
	user := &auth.User{
		ID:    "user-analyst",
		Name:  "Analyst User",
		Roles: []string{"analyst"},
	}

	// Attempt to authorize TIME_TRAVEL - this MUST succeed
	ctx := auth.ContextWithUser(context.Background(), user)
	err := authz.Authorize(ctx, user, []string{"analytics.sales_orders"}, capabilities.CapabilityTimeTravel)

	// GREEN-FLAG: Authorization must pass for authorized user with TIME_TRAVEL
	if err != nil {
		t.Errorf("GREEN-FLAG VIOLATION: Authorized user was denied TIME_TRAVEL!\n"+
			"User: %s\n"+
			"Role: analyst\n"+
			"Table: analytics.sales_orders\n"+
			"Capability: TIME_TRAVEL\n"+
			"Error: %v", user.ID, err)
	}
}

// TestAuthorization_MultiTableAllAuthorized proves that a query referencing
// multiple tables succeeds when user has permission on ALL tables.
//
// Green-Flag: Multi-table query where all tables are permitted.
func TestAuthorization_MultiTableAllAuthorized(t *testing.T) {
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

	// Create authorization service with permission for BOTH tables
	authz := auth.NewAuthorizationService()
	authz.GrantAccess("analyst", "analytics.sales_orders", capabilities.CapabilityRead)
	authz.GrantAccess("analyst", "analytics.payments", capabilities.CapabilityRead)

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

	// Attempt to authorize - this MUST succeed
	ctx := auth.ContextWithUser(context.Background(), user)
	err = authz.Authorize(ctx, user, logical.Tables, capabilities.CapabilityRead)

	// GREEN-FLAG: Authorization must pass when all tables are authorized
	if err != nil {
		t.Errorf("GREEN-FLAG VIOLATION: Authorized user was denied multi-table query!\n"+
			"User: %s\n"+
			"Role: analyst\n"+
			"Tables: %v\n"+
			"Error: %v", user.ID, logical.Tables, err)
	}
}

// TestAuthorization_MultipleRolesGrantAccess proves that a user with multiple
// roles can access tables if ANY role has permission.
func TestAuthorization_MultipleRolesGrantAccess(t *testing.T) {
	// Create authorization service with different permissions per role
	authz := auth.NewAuthorizationService()
	authz.GrantAccess("finance", "analytics.payments", capabilities.CapabilityRead)
	authz.GrantAccess("sales", "analytics.sales_orders", capabilities.CapabilityRead)

	// Create a user with BOTH roles
	user := &auth.User{
		ID:    "user-multi-role",
		Name:  "Multi Role User",
		Roles: []string{"finance", "sales"},
	}

	// Attempt to authorize both tables - this MUST succeed
	ctx := auth.ContextWithUser(context.Background(), user)

	// Check payments (via finance role)
	err := authz.Authorize(ctx, user, []string{"analytics.payments"}, capabilities.CapabilityRead)
	if err != nil {
		t.Errorf("GREEN-FLAG VIOLATION: Multi-role user denied access to table via finance role: %v", err)
	}

	// Check sales_orders (via sales role)
	err = authz.Authorize(ctx, user, []string{"analytics.sales_orders"}, capabilities.CapabilityRead)
	if err != nil {
		t.Errorf("GREEN-FLAG VIOLATION: Multi-role user denied access to table via sales role: %v", err)
	}
}

// TestAuthorization_GrantAndRevokeAccess proves that access can be granted and revoked.
func TestAuthorization_GrantAndRevokeAccess(t *testing.T) {
	authz := auth.NewAuthorizationService()

	user := &auth.User{
		ID:    "user-test",
		Name:  "Test User",
		Roles: []string{"tester"},
	}

	ctx := auth.ContextWithUser(context.Background(), user)

	// Initially, no access
	err := authz.Authorize(ctx, user, []string{"analytics.test_table"}, capabilities.CapabilityRead)
	if err == nil {
		t.Error("expected access denied before grant")
	}

	// Grant access
	authz.GrantAccess("tester", "analytics.test_table", capabilities.CapabilityRead)

	// Now should have access
	err = authz.Authorize(ctx, user, []string{"analytics.test_table"}, capabilities.CapabilityRead)
	if err != nil {
		t.Errorf("expected access after grant, got: %v", err)
	}

	// Revoke access
	authz.RevokeAccess("tester", "analytics.test_table", capabilities.CapabilityRead)

	// Should be denied again
	err = authz.Authorize(ctx, user, []string{"analytics.test_table"}, capabilities.CapabilityRead)
	if err == nil {
		t.Error("expected access denied after revoke")
	}
}
