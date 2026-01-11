package greenflag

import (
	"context"
	"testing"

	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/gateway"
	"github.com/canonica-labs/canonica/internal/planner"
	"github.com/canonica-labs/canonica/internal/router"
	"github.com/canonica-labs/canonica/internal/sql"
	"github.com/canonica-labs/canonica/internal/tables"
)

// =============================================================================
// Phase 1: SNAPSHOT_CONSISTENT Green-Flag Tests
// Per phase-1-spec.md: "These tests verify expected behavior after implementation"
// =============================================================================

// TestSnapshotConsistent_AcceptsQueryWithAsOf proves that queries on
// SNAPSHOT_CONSISTENT tables WITH AS OF are accepted.
//
// Green-Flag: SNAPSHOT_CONSISTENT with AS OF should pass planning.
func TestSnapshotConsistent_AcceptsQueryWithAsOf(t *testing.T) {
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

	// Act: Query WITH AS OF (satisfies SNAPSHOT_CONSISTENT)
	parser := sql.NewParser()
	plan, err := parser.Parse("SELECT * FROM events WHERE id = 1")
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}
	// Set time travel to satisfy SNAPSHOT_CONSISTENT
	plan.HasTimeTravel = true
	plan.TimeTravelTimestamp = "2024-01-01"

	// Assert: Planning should succeed
	execPlan, planErr := p.Plan(ctx, plan)
	if planErr != nil {
		t.Fatalf("expected planning to succeed with AS OF on SNAPSHOT_CONSISTENT table, got: %v", planErr)
	}

	// Verify plan is valid
	if execPlan == nil {
		t.Fatal("expected non-nil execution plan")
	}
	if execPlan.Engine != "duckdb" {
		t.Errorf("expected engine 'duckdb', got '%s'", execPlan.Engine)
	}
}

// TestSnapshotConsistent_AcceptsNonSnapshotTable proves that tables
// without SNAPSHOT_CONSISTENT constraint don't require AS OF.
//
// Green-Flag: Non-snapshot tables work without AS OF.
func TestSnapshotConsistent_AcceptsNonSnapshotTable(t *testing.T) {
	ctx := context.Background()

	// Arrange: Table WITHOUT SNAPSHOT_CONSISTENT constraint
	registry := gateway.NewInMemoryTableRegistry()
	vt := &tables.VirtualTable{
		Name:         "events",
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
		Constraints:  []capabilities.Constraint{}, // No SNAPSHOT_CONSISTENT
		Sources: []tables.PhysicalSource{{
			Engine:   "duckdb",
			Location: "s3://bucket/events",
			Format:   "parquet",
		}},
	}
	registry.Register(vt)

	// Arrange: Router
	r := router.NewRouter()
	r.RegisterEngine(&router.Engine{
		Name:         "duckdb",
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
		Available:    true,
		Priority:     1,
	})

	// Arrange: Planner
	p := planner.NewPlanner(registry, r)

	// Act: Query WITHOUT AS OF (should be fine since no SNAPSHOT_CONSISTENT)
	parser := sql.NewParser()
	plan, err := parser.Parse("SELECT * FROM events WHERE id = 1")
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	// Assert: Planning should succeed
	execPlan, planErr := p.Plan(ctx, plan)
	if planErr != nil {
		t.Fatalf("expected planning to succeed for non-snapshot table, got: %v", planErr)
	}

	if execPlan == nil {
		t.Fatal("expected non-nil execution plan")
	}
}

// TestSnapshotConsistent_AcceptsMultipleSnapshotTables proves that queries
// joining multiple SNAPSHOT_CONSISTENT tables are allowed when all constraints
// can be satisfied.
//
// Green-Flag: Multiple SNAPSHOT_CONSISTENT tables with AS OF should work.
func TestSnapshotConsistent_AcceptsMultipleSnapshotTables(t *testing.T) {
	ctx := context.Background()

	// Arrange: Two tables both with SNAPSHOT_CONSISTENT
	registry := gateway.NewInMemoryTableRegistry()

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

	vt2 := &tables.VirtualTable{
		Name:         "customers",
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead, capabilities.CapabilityTimeTravel},
		Constraints:  []capabilities.Constraint{capabilities.ConstraintSnapshotConsistent},
		Sources: []tables.PhysicalSource{{
			Engine:   "duckdb",
			Location: "s3://bucket/customers",
			Format:   "parquet",
		}},
	}
	registry.Register(vt2)

	// Arrange: Router with TIME_TRAVEL capable engine
	r := router.NewRouter()
	r.RegisterEngine(&router.Engine{
		Name:         "duckdb",
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead, capabilities.CapabilityTimeTravel},
		Available:    true,
		Priority:     1,
	})

	// Arrange: Planner
	p := planner.NewPlanner(registry, r)

	// Act: Query joining both tables WITH AS OF
	parser := sql.NewParser()
	plan, err := parser.Parse("SELECT * FROM orders JOIN customers ON orders.customer_id = customers.id")
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}
	// Set time travel to satisfy SNAPSHOT_CONSISTENT for both tables
	plan.HasTimeTravel = true
	plan.TimeTravelTimestamp = "2024-01-01"

	// Assert: Planning should succeed
	execPlan, planErr := p.Plan(ctx, plan)
	if planErr != nil {
		t.Fatalf("expected planning to succeed for multiple SNAPSHOT_CONSISTENT tables with AS OF, got: %v", planErr)
	}

	if execPlan == nil {
		t.Fatal("expected non-nil execution plan")
	}
}

// TestSnapshotConsistent_AcceptsSameTimestampPerTable proves that queries
// joining SNAPSHOT_CONSISTENT tables with the SAME per-table timestamp succeed.
//
// Green-Flag: Same timestamps across SNAPSHOT_CONSISTENT tables is consistent.
func TestSnapshotConsistent_AcceptsSameTimestampPerTable(t *testing.T) {
	ctx := context.Background()

	// Arrange: Two tables both with SNAPSHOT_CONSISTENT
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

	// Arrange: Router with TIME_TRAVEL capable engine
	r := router.NewRouter()
	r.RegisterEngine(&router.Engine{
		Name:         "iceberg-trino",
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead, capabilities.CapabilityTimeTravel},
		Available:    true,
		Priority:     1,
	})

	p := planner.NewPlanner(registry, r)

	// Act: Query joining both tables with SAME timestamp
	parser := sql.NewParser()
	plan, err := parser.Parse("SELECT * FROM orders FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01T00:00:00Z' JOIN customers FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01T00:00:00Z' ON orders.customer_id = customers.id")
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	// Verify parser extracted per-table timestamps
	if len(plan.TimeTravelPerTable) == 0 {
		t.Fatal("expected TimeTravelPerTable to be populated, got empty map")
	}

	// Assert: Planning should succeed with same timestamps
	execPlan, planErr := p.Plan(ctx, plan)
	if planErr != nil {
		t.Fatalf("expected planning to succeed for SNAPSHOT_CONSISTENT tables with same timestamps, got: %v", planErr)
	}

	if execPlan == nil {
		t.Fatal("expected non-nil execution plan")
	}
}
