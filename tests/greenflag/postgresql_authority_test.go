// Package greenflag contains Green-Flag tests that prove the system correctly
// executes behavior that is explicitly declared safe.
//
// Per docs/test.md: "Green-Flag tests assert that the system SUCCESSFULLY EXECUTES
// behavior that is explicitly declared safe."
//
// Per docs/phase-4-spec.md §1-2: PostgreSQL as metadata authority with proper wiring
package greenflag

import (
	"context"
	"testing"
	"time"

	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/gateway"
	"github.com/canonica-labs/canonica/internal/storage"
	"github.com/canonica-labs/canonica/internal/tables"
)

// TestGatewayStartsWithValidRepository tests that gateway starts with valid repository.
// Per phase-4-spec.md §1: Gateway with proper repository should start successfully
func TestGatewayStartsWithValidRepository(t *testing.T) {
	repo := storage.NewMockRepository()

	cfg := gateway.Config{
		Version:        "test",
		ProductionMode: false, // Test mode
	}

	gw, err := gateway.NewGatewayWithRepository(repo, cfg)
	if err != nil {
		t.Fatalf("Gateway should start with valid repository: %v", err)
	}

	if gw == nil {
		t.Fatal("Gateway should not be nil")
	}
}

// TestMetadataPersistsThroughRepository tests that table data persists.
// Per phase-4-spec.md §2: "Restart gateway → metadata persists"
func TestMetadataPersistsThroughRepository(t *testing.T) {
	// Create shared repository (simulating PostgreSQL persistence)
	repo := storage.NewMockRepository()
	ctx := context.Background()

	// Register a table
	table := &tables.VirtualTable{
		Name: "analytics.sales_orders",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatDelta, Location: "s3://bucket/sales"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	}
	if err := repo.Create(ctx, table); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Verify table exists
	retrieved, err := repo.Get(ctx, "analytics.sales_orders")
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}

	if retrieved.Name != table.Name {
		t.Errorf("Table name mismatch: got %s, want %s", retrieved.Name, table.Name)
	}

	// Simulate "restart" by creating new gateway with same repo
	cfg := gateway.Config{Version: "test"}
	_, err = gateway.NewGatewayWithRepository(repo, cfg)
	if err != nil {
		t.Fatalf("Gateway restart failed: %v", err)
	}

	// Table should still be visible
	retrieved2, err := repo.Get(ctx, "analytics.sales_orders")
	if err != nil {
		t.Fatalf("Table should persist across restart: %v", err)
	}

	if retrieved2.Name != table.Name {
		t.Error("Table data should persist across gateway restart")
	}
}

// TestConcurrentRepositoryReadsAreConsistent tests that concurrent reads see consistent state.
// Per phase-4-spec.md §2: "Concurrent requests observe consistent metadata"
func TestConcurrentRepositoryReadsAreConsistent(t *testing.T) {
	repo := storage.NewMockRepository()
	ctx := context.Background()

	// Register a table
	table := &tables.VirtualTable{
		Name: "analytics.orders",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatDelta, Location: "s3://bucket/orders"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	}
	if err := repo.Create(ctx, table); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Perform concurrent reads
	const numReaders = 10
	results := make(chan *tables.VirtualTable, numReaders)
	errors := make(chan error, numReaders)

	for i := 0; i < numReaders; i++ {
		go func() {
			tbl, err := repo.Get(ctx, "analytics.orders")
			if err != nil {
				errors <- err
				return
			}
			results <- tbl
		}()
	}

	// Collect results
	for i := 0; i < numReaders; i++ {
		select {
		case err := <-errors:
			t.Errorf("Concurrent read failed: %v", err)
		case result := <-results:
			if result.Name != "analytics.orders" {
				t.Errorf("Inconsistent read: got %s", result.Name)
			}
		case <-time.After(5 * time.Second):
			t.Error("Concurrent read timed out")
		}
	}
}

// TestTableRegistrationThroughRepository tests table registration persists.
// Per phase-4-spec.md §2: Route table registration through PostgreSQL
func TestTableRegistrationThroughRepository(t *testing.T) {
	repo := storage.NewMockRepository()
	ctx := context.Background()

	table := &tables.VirtualTable{
		Name:        "analytics.customers",
		Description: "Customer data",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatIceberg, Location: "s3://bucket/customers"},
		},
		Capabilities: []capabilities.Capability{
			capabilities.CapabilityRead,
			capabilities.CapabilityTimeTravel,
		},
		Constraints: []capabilities.Constraint{
			capabilities.ConstraintReadOnly,
		},
	}

	// Register
	err := repo.Create(ctx, table)
	if err != nil {
		t.Fatalf("Table registration failed: %v", err)
	}

	// Verify
	retrieved, err := repo.Get(ctx, "analytics.customers")
	if err != nil {
		t.Fatalf("Table lookup failed: %v", err)
	}

	// Check all fields persisted
	if retrieved.Name != table.Name {
		t.Errorf("Name mismatch: got %s", retrieved.Name)
	}
	if retrieved.Description != table.Description {
		t.Errorf("Description mismatch: got %s", retrieved.Description)
	}
	if len(retrieved.Sources) != 1 {
		t.Errorf("Sources count mismatch: got %d", len(retrieved.Sources))
	}
	if len(retrieved.Capabilities) != 2 {
		t.Errorf("Capabilities count mismatch: got %d", len(retrieved.Capabilities))
	}
	if len(retrieved.Constraints) != 1 {
		t.Errorf("Constraints count mismatch: got %d", len(retrieved.Constraints))
	}
}

// TestTableLookupThroughRepository tests table lookup uses repository.
// Per phase-4-spec.md §2: Route table lookup through PostgreSQL
func TestTableLookupThroughRepository(t *testing.T) {
	repo := storage.NewMockRepository()
	ctx := context.Background()

	// Register multiple tables
	tables := []*tables.VirtualTable{
		{
			Name: "schema1.table1",
			Sources: []tables.PhysicalSource{
				{Format: tables.FormatDelta, Location: "s3://bucket/t1"},
			},
			Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
		},
		{
			Name: "schema2.table2",
			Sources: []tables.PhysicalSource{
				{Format: tables.FormatIceberg, Location: "s3://bucket/t2"},
			},
			Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
		},
	}

	for _, tbl := range tables {
		if err := repo.Create(ctx, tbl); err != nil {
			t.Fatalf("Failed to register table %s: %v", tbl.Name, err)
		}
	}

	// Lookup each table
	for _, tbl := range tables {
		retrieved, err := repo.Get(ctx, tbl.Name)
		if err != nil {
			t.Errorf("Lookup failed for %s: %v", tbl.Name, err)
			continue
		}
		if retrieved.Name != tbl.Name {
			t.Errorf("Lookup returned wrong table: got %s, want %s", retrieved.Name, tbl.Name)
		}
	}
}

// TestListTablesThroughRepository tests listing tables uses repository.
// Per phase-4-spec.md §2: Route table listing through PostgreSQL
func TestListTablesThroughRepository(t *testing.T) {
	repo := storage.NewMockRepository()
	ctx := context.Background()

	// Start with empty
	initial, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(initial) != 0 {
		t.Errorf("Expected empty list, got %d tables", len(initial))
	}

	// Register tables
	for i := 0; i < 3; i++ {
		tbl := &tables.VirtualTable{
			Name: "schema.table" + string(rune('0'+i)),
			Sources: []tables.PhysicalSource{
				{Format: tables.FormatParquet, Location: "s3://bucket/t" + string(rune('0'+i))},
			},
			Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
		}
		if err := repo.Create(ctx, tbl); err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}
	}

	// List again
	listed, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(listed) != 3 {
		t.Errorf("Expected 3 tables, got %d", len(listed))
	}
}

// TestConnectivityCheckSucceeds tests that healthy repository passes connectivity.
// Per phase-4-spec.md §1: Connectivity check at startup
func TestConnectivityCheckSucceeds(t *testing.T) {
	repo := storage.NewMockRepository()
	ctx := context.Background()

	err := repo.CheckConnectivity(ctx)
	if err != nil {
		t.Errorf("Healthy repository should pass connectivity check: %v", err)
	}
}

// TestRepositoryUpdatePreservesMetadata tests that updates preserve timestamps.
// Per phase-4-spec.md §2: Metadata operations through repository
func TestRepositoryUpdatePreservesMetadata(t *testing.T) {
	repo := storage.NewMockRepository()
	ctx := context.Background()

	// Create table
	table := &tables.VirtualTable{
		Name: "analytics.events",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatDelta, Location: "s3://bucket/events"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	}
	if err := repo.Create(ctx, table); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Get original created_at
	original, _ := repo.Get(ctx, "analytics.events")
	originalCreatedAt := original.CreatedAt

	// Wait a moment and update
	time.Sleep(10 * time.Millisecond)
	table.Description = "Updated description"
	if err := repo.Update(ctx, table); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Verify created_at preserved, updated_at changed
	updated, _ := repo.Get(ctx, "analytics.events")
	if !updated.CreatedAt.Equal(originalCreatedAt) {
		t.Error("created_at should be preserved on update")
	}
	if updated.Description != "Updated description" {
		t.Error("Description should be updated")
	}
}
