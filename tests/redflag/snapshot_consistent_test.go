package redflag

import (
	"context"
	"strings"
	"testing"

	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/gateway"
	"github.com/canonica-labs/canonica/internal/planner"
	"github.com/canonica-labs/canonica/internal/router"
	"github.com/canonica-labs/canonica/internal/sql"
	"github.com/canonica-labs/canonica/internal/tables"
)

// =============================================================================
// Phase 1: SNAPSHOT_CONSISTENT Red-Flag Tests
// Per phase-1-spec.md: "These tests must fail before implementation"
// =============================================================================

// TestSnapshotConsistent_RejectsQueryWithoutAsOf proves that queries on
// SNAPSHOT_CONSISTENT tables without AS OF are rejected.
//
// Red-Flag: SNAPSHOT_CONSISTENT without AS OF violates consistency guarantee.
// This MUST fail before enforcement is implemented.
func TestSnapshotConsistent_RejectsQueryWithoutAsOf(t *testing.T) {
	ctx := context.Background()

	// Arrange: Table with SNAPSHOT_CONSISTENT constraint
	registry := gateway.NewInMemoryTableRegistry()
	vt := &tables.VirtualTable{
		Name:         "events",
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead, capabilities.CapabilityTimeTravel},
		Constraints:  []capabilities.Constraint{capabilities.ConstraintSnapshotConsistent},
		Sources: []tables.PhysicalSource{{
			Engine:   "duckdb",
			Location: "s3://bucket/events",
			Format:   "parquet",
		}},
	}
	registry.Register(vt)

	// Arrange: Router with engine that has TIME_TRAVEL
	r := router.NewRouter()
	r.RegisterEngine(&router.Engine{
		Name:         "duckdb",
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead, capabilities.CapabilityTimeTravel},
		Available:    true,
		Priority:     1,
	})

	// Arrange: Planner
	p := planner.NewPlanner(registry, r)

	// Act: Query WITHOUT AS OF (violation of SNAPSHOT_CONSISTENT)
	parser := sql.NewParser()
	plan, err := parser.Parse("SELECT * FROM events WHERE id = 1")
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	// Assert: Planning MUST fail because SNAPSHOT_CONSISTENT requires AS OF
	_, planErr := p.Plan(ctx, plan)
	if planErr == nil {
		t.Fatal("expected error: SNAPSHOT_CONSISTENT table queried without AS OF, but got nil")
	}

	// The error should mention snapshot consistency
	if !containsAny(planErr.Error(), "snapshot", "SNAPSHOT_CONSISTENT", "AS OF") {
		t.Errorf("expected error to mention snapshot consistency, got: %v", planErr)
	}
}

// TestSnapshotConsistent_RejectsEngineWithoutTimeTravel proves that
// SNAPSHOT_CONSISTENT tables cannot be routed to engines without TIME_TRAVEL.
//
// Red-Flag: Routing to non-snapshot engine violates consistency guarantee.
func TestSnapshotConsistent_RejectsEngineWithoutTimeTravel(t *testing.T) {
	ctx := context.Background()

	// Arrange: Table with SNAPSHOT_CONSISTENT constraint
	registry := gateway.NewInMemoryTableRegistry()
	vt := &tables.VirtualTable{
		Name:         "events",
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead}, // Table needs read
		Constraints:  []capabilities.Constraint{capabilities.ConstraintSnapshotConsistent},
		Sources: []tables.PhysicalSource{{
			Engine:   "trino",
			Location: "catalog.schema.events",
			Format:   "iceberg",
		}},
	}
	registry.Register(vt)

	// Arrange: Router with engine that does NOT have TIME_TRAVEL
	r := router.NewRouter()
	r.RegisterEngine(&router.Engine{
		Name:         "trino",
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead}, // No TIME_TRAVEL
		Available:    true,
		Priority:     1,
	})

	// Arrange: Planner
	p := planner.NewPlanner(registry, r)

	// Act: Query WITH AS OF (but engine can't handle it)
	parser := sql.NewParser()
	// Note: Using a simple query, then manually setting time travel
	plan, err := parser.Parse("SELECT * FROM events")
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}
	// Manually set time travel since parser may not support the syntax
	plan.HasTimeTravel = true
	plan.TimeTravelTimestamp = "2024-01-01"

	// Assert: Planning MUST fail because engine lacks TIME_TRAVEL
	_, planErr := p.Plan(ctx, plan)
	if planErr == nil {
		t.Fatal("expected error: SNAPSHOT_CONSISTENT requires TIME_TRAVEL engine, but got nil")
	}
}

// TestSnapshotConsistent_RejectsMixedSnapshotCapabilities proves that
// queries joining SNAPSHOT_CONSISTENT tables across engines with different
// snapshot capabilities are rejected.
//
// Red-Flag: Mixed snapshot capabilities break consistency guarantee.
func TestSnapshotConsistent_RejectsMixedSnapshotCapabilities(t *testing.T) {
	ctx := context.Background()

	// Arrange: Two tables - one SNAPSHOT_CONSISTENT, one not
	registry := gateway.NewInMemoryTableRegistry()

	// Table 1: SNAPSHOT_CONSISTENT
	vt1 := &tables.VirtualTable{
		Name:         "orders",
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead, capabilities.CapabilityTimeTravel},
		Constraints:  []capabilities.Constraint{capabilities.ConstraintSnapshotConsistent},
		Sources: []tables.PhysicalSource{{
			Engine:   "duckdb",
			Location: "s3://bucket/orders",
			Format:   "parquet",
		}},
	}
	registry.Register(vt1)

	// Table 2: No SNAPSHOT_CONSISTENT
	vt2 := &tables.VirtualTable{
		Name:         "customers",
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
		Constraints:  []capabilities.Constraint{}, // No snapshot constraint
		Sources: []tables.PhysicalSource{{
			Engine:   "trino",
			Location: "catalog.schema.customers",
			Format:   "parquet",
		}},
	}
	registry.Register(vt2)

	// Arrange: Router with mixed capabilities
	r := router.NewRouter()
	r.RegisterEngine(&router.Engine{
		Name:         "duckdb",
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead, capabilities.CapabilityTimeTravel},
		Available:    true,
		Priority:     1,
	})
	r.RegisterEngine(&router.Engine{
		Name:         "trino",
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead}, // No TIME_TRAVEL
		Available:    true,
		Priority:     2,
	})

	// Arrange: Planner
	p := planner.NewPlanner(registry, r)

	// Act: Query joining both tables
	parser := sql.NewParser()
	plan, err := parser.Parse("SELECT * FROM orders JOIN customers ON orders.customer_id = customers.id")
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	// Assert: Planning MUST fail due to mixed snapshot constraints
	_, planErr := p.Plan(ctx, plan)
	if planErr == nil {
		t.Fatal("expected error: mixed SNAPSHOT_CONSISTENT tables in join, but got nil")
	}
}

// TestSnapshotConsistent_RejectsSnapshotMismatch proves that queries
// joining multiple SNAPSHOT_CONSISTENT tables with different snapshot
// timestamps are rejected.
//
// Red-Flag: Different snapshot timestamps break consistency guarantee.
func TestSnapshotConsistent_RejectsSnapshotMismatch(t *testing.T) {
	ctx := context.Background()

	// Arrange: Tables with SNAPSHOT_CONSISTENT constraint
	registry := gateway.NewInMemoryTableRegistry()
	registry.Register(&tables.VirtualTable{
		Name:         "orders",
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead, capabilities.CapabilityTimeTravel},
		Constraints:  []capabilities.Constraint{capabilities.ConstraintSnapshotConsistent},
		Sources: []tables.PhysicalSource{{
			Engine:   "iceberg-trino",
			Location: "catalog.schema.orders",
			Format:   "iceberg",
		}},
	})
	registry.Register(&tables.VirtualTable{
		Name:         "customers",
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead, capabilities.CapabilityTimeTravel},
		Constraints:  []capabilities.Constraint{capabilities.ConstraintSnapshotConsistent},
		Sources: []tables.PhysicalSource{{
			Engine:   "iceberg-trino",
			Location: "catalog.schema.customers",
			Format:   "iceberg",
		}},
	})

	// Arrange: Router with TIME_TRAVEL capability
	r := router.NewRouter()
	r.RegisterEngine(&router.Engine{
		Name:         "iceberg-trino",
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead, capabilities.CapabilityTimeTravel},
		Available:    true,
		Priority:     1,
	})

	p := planner.NewPlanner(registry, r)

	// Act: Query joining both tables with DIFFERENT timestamps
	parser := sql.NewParser()
	plan, err := parser.Parse("SELECT * FROM orders FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01T00:00:00Z' JOIN customers FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-02T00:00:00Z' ON orders.customer_id = customers.id")
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	// Verify parser extracted per-table timestamps
	if len(plan.TimeTravelPerTable) == 0 {
		t.Fatal("expected TimeTravelPerTable to be populated, got empty map")
	}

	// Assert: Planning MUST fail due to different snapshot timestamps
	_, planErr := p.Plan(ctx, plan)
	if planErr == nil {
		t.Fatal("expected error: SNAPSHOT_CONSISTENT tables with different timestamps, but got nil")
	}

	// Verify error mentions snapshot timestamp mismatch
	if !containsAny(planErr.Error(), "snapshot", "timestamp", "consistent") {
		t.Errorf("expected error about snapshot timestamps, got: %v", planErr)
	}
}

// containsAny checks if the string contains any of the substrings.
func containsAny(s string, substrs ...string) bool {
	for _, sub := range substrs {
		if containsIgnoreCase(s, sub) {
			return true
		}
	}
	return false
}

// containsIgnoreCase checks if s contains substr (case-insensitive).
func containsIgnoreCase(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}
