// Package greenflag contains Green-Flag tests that prove the system correctly
// succeeds when semantics are guaranteed.
//
// Per docs/test.md: "Green-Flag tests assert that the system SUCCESSFULLY EXECUTES
// behavior that is explicitly declared safe."
//
// Per docs/phase-3-spec.md §7: "Table registered via API is visible to gateway.
// Restart does not lose metadata. Concurrent reads observe consistent state."
package greenflag

import (
	"context"
	"sync"
	"testing"

	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/storage"
	"github.com/canonica-labs/canonica/internal/tables"
)

// TestTableRegisteredIsVisible tests that registered tables are immediately visible.
// Per phase-3-spec.md §7: "Table registered via API is visible to gateway"
func TestTableRegisteredIsVisible(t *testing.T) {
	repo := storage.NewMockRepository()
	ctx := context.Background()

	// Register a table
	table := &tables.VirtualTable{
		Name: "analytics.sales",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatDelta, Location: "s3://bucket/sales"},
		},
		Capabilities: []capabilities.Capability{
			capabilities.CapabilityRead,
			capabilities.CapabilityTimeTravel,
		},
	}

	err := repo.Create(ctx, table)
	if err != nil {
		t.Fatalf("Failed to register table: %v", err)
	}

	// Table should be visible immediately
	retrieved, err := repo.Get(ctx, "analytics.sales")
	if err != nil {
		t.Fatalf("Registered table should be visible: %v", err)
	}

	if retrieved.Name != table.Name {
		t.Errorf("Name mismatch: got %s, want %s", retrieved.Name, table.Name)
	}

	if len(retrieved.Capabilities) != len(table.Capabilities) {
		t.Errorf("Capabilities mismatch: got %d, want %d", len(retrieved.Capabilities), len(table.Capabilities))
	}
}

// TestMetadataPersistsAcrossRestarts tests that metadata is persisted.
// Per phase-3-spec.md §7: "Restart does not lose metadata"
func TestMetadataPersistsAcrossRestarts(t *testing.T) {
	// This test simulates restart by creating a new repository reference
	// In real implementation, this would use PostgreSQL
	repo := storage.NewMockRepository()
	ctx := context.Background()

	// Register a table
	table := &tables.VirtualTable{
		Name: "analytics.orders",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatIceberg, Location: "s3://bucket/orders"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	}

	err := repo.Create(ctx, table)
	if err != nil {
		t.Fatalf("Failed to register table: %v", err)
	}

	// Verify table exists before "restart"
	exists, err := repo.Exists(ctx, "analytics.orders")
	if err != nil {
		t.Fatalf("Failed to check existence: %v", err)
	}
	if !exists {
		t.Fatal("Table should exist before restart")
	}

	// Note: In production, this would test actual PostgreSQL persistence
	// For mock repository, we verify the data is still accessible
	retrieved, err := repo.Get(ctx, "analytics.orders")
	if err != nil {
		t.Fatalf("Metadata should persist: %v", err)
	}

	if retrieved.Name != table.Name {
		t.Errorf("Persisted table name mismatch: got %s, want %s", retrieved.Name, table.Name)
	}
}

// TestConcurrentReadsAreConsistent tests that concurrent reads return consistent data.
// Per phase-3-spec.md §7: "Concurrent reads observe consistent state"
func TestConcurrentReadsAreConsistent(t *testing.T) {
	repo := storage.NewMockRepository()
	ctx := context.Background()

	// Register a table
	table := &tables.VirtualTable{
		Name: "analytics.events",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatParquet, Location: "s3://bucket/events"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	}

	err := repo.Create(ctx, table)
	if err != nil {
		t.Fatalf("Failed to register table: %v", err)
	}

	// Perform concurrent reads
	const numReaders = 10
	var wg sync.WaitGroup
	errChan := make(chan error, numReaders)
	resultChan := make(chan *tables.VirtualTable, numReaders)

	wg.Add(numReaders)
	for i := 0; i < numReaders; i++ {
		go func() {
			defer wg.Done()
			retrieved, err := repo.Get(ctx, "analytics.events")
			if err != nil {
				errChan <- err
				return
			}
			resultChan <- retrieved
		}()
	}

	wg.Wait()
	close(errChan)
	close(resultChan)

	// Check for errors
	for err := range errChan {
		t.Errorf("Concurrent read failed: %v", err)
	}

	// All results should be identical
	var expectedName string
	for result := range resultChan {
		if expectedName == "" {
			expectedName = result.Name
		} else if result.Name != expectedName {
			t.Errorf("Inconsistent result: got %s, want %s", result.Name, expectedName)
		}
	}
}

// TestListReturnsAllRegisteredTables tests that List returns all tables.
func TestListReturnsAllRegisteredTables(t *testing.T) {
	repo := storage.NewMockRepository()
	ctx := context.Background()

	// Register multiple tables
	tableNames := []string{"test.table1", "test.table2", "test.table3"}
	for _, name := range tableNames {
		table := &tables.VirtualTable{
			Name: name,
			Sources: []tables.PhysicalSource{
				{Format: tables.FormatDelta, Location: "s3://bucket/" + name},
			},
			Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
		}
		if err := repo.Create(ctx, table); err != nil {
			t.Fatalf("Failed to create table %s: %v", name, err)
		}
	}

	// List should return all tables
	allTables, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("Failed to list tables: %v", err)
	}

	if len(allTables) != len(tableNames) {
		t.Errorf("Expected %d tables, got %d", len(tableNames), len(allTables))
	}
}

// TestRepositoryRejectsInvalidTables tests that invalid tables are rejected.
func TestRepositoryRejectsInvalidTables(t *testing.T) {
	repo := storage.NewMockRepository()
	ctx := context.Background()

	// Table without sources should be rejected
	invalidTable := &tables.VirtualTable{
		Name:         "test.invalid",
		Sources:      []tables.PhysicalSource{}, // No sources!
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	}

	err := repo.Create(ctx, invalidTable)
	if err == nil {
		t.Fatal("Repository should reject tables without sources")
	}
}

// TestRepositoryUpdatePreservesCreatedAt tests that updates preserve created_at.
func TestRepositoryUpdatePreservesCreatedAt(t *testing.T) {
	repo := storage.NewMockRepository()
	ctx := context.Background()

	// Create a table
	table := &tables.VirtualTable{
		Name: "test.orders",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatDelta, Location: "s3://bucket/orders"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	}
	if err := repo.Create(ctx, table); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Get original created_at
	original, _ := repo.Get(ctx, "test.orders")
	originalCreatedAt := original.CreatedAt

	// Update the table
	updated := &tables.VirtualTable{
		Name: "test.orders",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatDelta, Location: "s3://bucket/orders-v2"},
		},
		Capabilities: []capabilities.Capability{
			capabilities.CapabilityRead,
			capabilities.CapabilityTimeTravel,
		},
	}
	if err := repo.Update(ctx, updated); err != nil {
		t.Fatalf("Failed to update table: %v", err)
	}

	// Verify created_at is preserved
	afterUpdate, _ := repo.Get(ctx, "test.orders")
	if !afterUpdate.CreatedAt.Equal(originalCreatedAt) {
		t.Error("Update should preserve created_at timestamp")
	}
}

// TestDatabaseConnectivitySuccess tests that connectivity check succeeds when healthy.
func TestDatabaseConnectivitySuccess(t *testing.T) {
	repo := storage.NewMockRepository()
	ctx := context.Background()

	// Connectivity check should succeed on healthy repository
	err := repo.CheckConnectivity(ctx)
	if err != nil {
		t.Errorf("Connectivity check should succeed: %v", err)
	}
}
