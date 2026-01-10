// Package greenflag contains tests that verify the system correctly ALLOWS safe behavior.
// These tests prove that valid operations succeed.
//
// Per docs/test.md: "Green-Flag tests must pass after implementation."
package greenflag

import (
	"context"
	"testing"

	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/storage"
	"github.com/canonica-labs/canonica/internal/tables"
)

// TestStorage_CreateTable verifies that valid tables can be created.
//
// Green-Flag: Valid table creation must succeed.
func TestStorage_CreateTable(t *testing.T) {
	repo := storage.NewMockRepository()
	ctx := context.Background()

	table := &tables.VirtualTable{
		Name:        "orders",
		Description: "Customer orders table",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatDelta, Location: "s3://bucket/orders"},
		},
		Capabilities: []capabilities.Capability{
			capabilities.CapabilityRead,
			capabilities.CapabilityTimeTravel,
		},
		Constraints: []capabilities.Constraint{
			capabilities.ConstraintReadOnly,
		},
	}

	err := repo.Create(ctx, table)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Verify table was created
	retrieved, err := repo.Get(ctx, "orders")
	if err != nil {
		t.Fatalf("failed to get created table: %v", err)
	}
	if retrieved.Name != "orders" {
		t.Errorf("expected name 'orders', got '%s'", retrieved.Name)
	}
	if retrieved.Description != "Customer orders table" {
		t.Errorf("expected description 'Customer orders table', got '%s'", retrieved.Description)
	}
}

// TestStorage_GetTable verifies that tables can be retrieved by name.
//
// Green-Flag: Valid table retrieval must succeed.
func TestStorage_GetTable(t *testing.T) {
	repo := storage.NewMockRepository()
	ctx := context.Background()

	// Create table first
	table := &tables.VirtualTable{
		Name: "products",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatIceberg, Location: "s3://bucket/products"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	}
	err := repo.Create(ctx, table)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Retrieve table
	retrieved, err := repo.Get(ctx, "products")
	if err != nil {
		t.Fatalf("failed to get table: %v", err)
	}

	if retrieved.Name != "products" {
		t.Errorf("expected name 'products', got '%s'", retrieved.Name)
	}
	if len(retrieved.Sources) != 1 {
		t.Errorf("expected 1 source, got %d", len(retrieved.Sources))
	}
	if retrieved.Sources[0].Format != tables.FormatIceberg {
		t.Errorf("expected format ICEBERG, got %s", retrieved.Sources[0].Format)
	}
}

// TestStorage_UpdateTable verifies that tables can be updated.
//
// Green-Flag: Valid table updates must succeed.
func TestStorage_UpdateTable(t *testing.T) {
	repo := storage.NewMockRepository()
	ctx := context.Background()

	// Create table first
	table := &tables.VirtualTable{
		Name: "customers",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatParquet, Location: "s3://bucket/customers"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	}
	err := repo.Create(ctx, table)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Update table
	table.Description = "Updated customer records"
	table.Capabilities = append(table.Capabilities, capabilities.CapabilityTimeTravel)

	err = repo.Update(ctx, table)
	if err != nil {
		t.Fatalf("failed to update table: %v", err)
	}

	// Verify update
	retrieved, err := repo.Get(ctx, "customers")
	if err != nil {
		t.Fatalf("failed to get updated table: %v", err)
	}
	if retrieved.Description != "Updated customer records" {
		t.Errorf("expected updated description, got '%s'", retrieved.Description)
	}
	if len(retrieved.Capabilities) != 2 {
		t.Errorf("expected 2 capabilities after update, got %d", len(retrieved.Capabilities))
	}
}

// TestStorage_DeleteTable verifies that tables can be deleted.
//
// Green-Flag: Valid table deletion must succeed.
func TestStorage_DeleteTable(t *testing.T) {
	repo := storage.NewMockRepository()
	ctx := context.Background()

	// Create table first
	table := &tables.VirtualTable{
		Name: "temp_data",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatDelta, Location: "s3://bucket/temp"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	}
	err := repo.Create(ctx, table)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Delete table
	err = repo.Delete(ctx, "temp_data")
	if err != nil {
		t.Fatalf("failed to delete table: %v", err)
	}

	// Verify deletion
	_, err = repo.Get(ctx, "temp_data")
	if err == nil {
		t.Error("expected error after deletion, got nil")
	}
}

// TestStorage_ListTables verifies that all tables can be listed.
//
// Green-Flag: Listing tables must return all registered tables.
func TestStorage_ListTables(t *testing.T) {
	repo := storage.NewMockRepository()
	ctx := context.Background()

	// Create multiple tables
	tableNames := []string{"table_a", "table_b", "table_c"}
	for _, name := range tableNames {
		table := &tables.VirtualTable{
			Name: name,
			Sources: []tables.PhysicalSource{
				{Format: tables.FormatDelta, Location: "s3://bucket/" + name},
			},
			Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
		}
		err := repo.Create(ctx, table)
		if err != nil {
			t.Fatalf("failed to create table %s: %v", name, err)
		}
	}

	// List tables
	tables, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("failed to list tables: %v", err)
	}

	if len(tables) != 3 {
		t.Errorf("expected 3 tables, got %d", len(tables))
	}

	// Verify all tables are present
	nameMap := make(map[string]bool)
	for _, tbl := range tables {
		nameMap[tbl.Name] = true
	}
	for _, name := range tableNames {
		if !nameMap[name] {
			t.Errorf("table %s not found in list", name)
		}
	}
}

// TestStorage_ListEmptyRepository verifies that listing an empty repository
// returns an empty list, not an error.
//
// Green-Flag: Listing empty repository must return empty list.
func TestStorage_ListEmptyRepository(t *testing.T) {
	repo := storage.NewMockRepository()
	ctx := context.Background()

	tables, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("failed to list empty repository: %v", err)
	}

	if len(tables) != 0 {
		t.Errorf("expected 0 tables, got %d", len(tables))
	}
}

// TestStorage_CreateTableWithMultipleSources verifies that tables with
// multiple physical sources can be created.
//
// Green-Flag: Multi-source tables must be supported.
func TestStorage_CreateTableWithMultipleSources(t *testing.T) {
	repo := storage.NewMockRepository()
	ctx := context.Background()

	table := &tables.VirtualTable{
		Name: "multi_format_table",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatDelta, Location: "s3://bucket/delta"},
			{Format: tables.FormatIceberg, Location: "s3://bucket/iceberg"},
			{Format: tables.FormatParquet, Location: "s3://bucket/parquet"},
		},
		Capabilities: []capabilities.Capability{
			capabilities.CapabilityRead,
			capabilities.CapabilityTimeTravel,
		},
	}

	err := repo.Create(ctx, table)
	if err != nil {
		t.Fatalf("failed to create multi-source table: %v", err)
	}

	retrieved, err := repo.Get(ctx, "multi_format_table")
	if err != nil {
		t.Fatalf("failed to get multi-source table: %v", err)
	}

	if len(retrieved.Sources) != 3 {
		t.Errorf("expected 3 sources, got %d", len(retrieved.Sources))
	}
}

// TestStorage_TableTimestamps verifies that created_at and updated_at
// timestamps are set correctly.
//
// Green-Flag: Timestamps must be set on create/update.
func TestStorage_TableTimestamps(t *testing.T) {
	repo := storage.NewMockRepository()
	ctx := context.Background()

	table := &tables.VirtualTable{
		Name: "timestamped",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatDelta, Location: "s3://bucket/data"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	}

	err := repo.Create(ctx, table)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	retrieved, err := repo.Get(ctx, "timestamped")
	if err != nil {
		t.Fatalf("failed to get table: %v", err)
	}

	if retrieved.CreatedAt.IsZero() {
		t.Error("expected CreatedAt to be set")
	}
	if retrieved.UpdatedAt.IsZero() {
		t.Error("expected UpdatedAt to be set")
	}
}

// TestStorage_ExistsByName verifies the Exists helper method works.
//
// Green-Flag: Exists check must work correctly.
func TestStorage_ExistsByName(t *testing.T) {
	repo := storage.NewMockRepository()
	ctx := context.Background()

	// Check non-existent
	exists, err := repo.Exists(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("failed to check exists: %v", err)
	}
	if exists {
		t.Error("expected non-existent table to return false")
	}

	// Create table
	table := &tables.VirtualTable{
		Name: "exists_test",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatDelta, Location: "s3://bucket/data"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	}
	err = repo.Create(ctx, table)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Check exists
	exists, err = repo.Exists(ctx, "exists_test")
	if err != nil {
		t.Fatalf("failed to check exists: %v", err)
	}
	if !exists {
		t.Error("expected existing table to return true")
	}
}

// TestStorage_RepositoryInterface verifies the interface is properly implemented.
//
// Green-Flag: Interface must be implemented correctly.
func TestStorage_RepositoryInterface(t *testing.T) {
	var _ storage.TableRepository = storage.NewMockRepository()
}
