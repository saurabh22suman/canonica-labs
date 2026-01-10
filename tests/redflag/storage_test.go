// Package redflag contains tests that verify the system correctly REJECTS unsafe behavior.
// These tests MUST fail before implementation (Red-Flag TDD).
//
// Per docs/test.md: "If it doesn't fail first, it doesn't prove safety."
package redflag

import (
	"context"
	"testing"
	"time"

	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/storage"
	"github.com/canonica-labs/canonica/internal/tables"
)

// TestStorage_RejectsDuplicateTable verifies that registering a table with
// a name that already exists is rejected.
//
// Red-Flag: Duplicate table names must be rejected.
func TestStorage_RejectsDuplicateTable(t *testing.T) {
	repo := storage.NewMockRepository()
	ctx := context.Background()

	// Register first table
	table1 := &tables.VirtualTable{
		Name: "orders",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatDelta, Location: "s3://bucket/orders"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	}
	err := repo.Create(ctx, table1)
	if err != nil {
		t.Fatalf("failed to create first table: %v", err)
	}

	// Attempt to register duplicate
	table2 := &tables.VirtualTable{
		Name: "orders", // Same name
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatIceberg, Location: "s3://bucket/orders-v2"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	}
	err = repo.Create(ctx, table2)
	if err == nil {
		t.Error("expected error for duplicate table name, got nil")
	}
}

// TestStorage_RejectsInvalidTableDefinition verifies that invalid table
// definitions are rejected during registration.
//
// Red-Flag: Invalid table definitions must be rejected.
func TestStorage_RejectsInvalidTableDefinition(t *testing.T) {
	repo := storage.NewMockRepository()
	ctx := context.Background()

	tests := []struct {
		name  string
		table *tables.VirtualTable
	}{
		{
			name: "empty name",
			table: &tables.VirtualTable{
				Name: "",
				Sources: []tables.PhysicalSource{
					{Format: tables.FormatDelta, Location: "s3://bucket/data"},
				},
			},
		},
		{
			name: "no sources",
			table: &tables.VirtualTable{
				Name:    "empty_sources",
				Sources: []tables.PhysicalSource{},
			},
		},
		{
			name: "source without location",
			table: &tables.VirtualTable{
				Name: "no_location",
				Sources: []tables.PhysicalSource{
					{Format: tables.FormatDelta, Location: ""},
				},
			},
		},
		{
			name: "invalid format",
			table: &tables.VirtualTable{
				Name: "invalid_format",
				Sources: []tables.PhysicalSource{
					{Format: "INVALID", Location: "s3://bucket/data"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := repo.Create(ctx, tt.table)
			if err == nil {
				t.Errorf("expected error for %s, got nil", tt.name)
			}
		})
	}
}

// TestStorage_RejectsUpdateNonExistent verifies that updating a table that
// doesn't exist is rejected.
//
// Red-Flag: Updating non-existent tables must be rejected.
func TestStorage_RejectsUpdateNonExistent(t *testing.T) {
	repo := storage.NewMockRepository()
	ctx := context.Background()

	table := &tables.VirtualTable{
		Name: "nonexistent",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatDelta, Location: "s3://bucket/data"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	}

	err := repo.Update(ctx, table)
	if err == nil {
		t.Error("expected error for updating non-existent table, got nil")
	}
}

// TestStorage_RejectsDeleteNonExistent verifies that deleting a table that
// doesn't exist is rejected.
//
// Red-Flag: Deleting non-existent tables must be rejected.
func TestStorage_RejectsDeleteNonExistent(t *testing.T) {
	repo := storage.NewMockRepository()
	ctx := context.Background()

	err := repo.Delete(ctx, "nonexistent_table")
	if err == nil {
		t.Error("expected error for deleting non-existent table, got nil")
	}
}

// TestStorage_RejectsGetNonExistent verifies that retrieving a table that
// doesn't exist returns an appropriate error.
//
// Red-Flag: Getting non-existent tables must return error.
func TestStorage_RejectsGetNonExistent(t *testing.T) {
	repo := storage.NewMockRepository()
	ctx := context.Background()

	_, err := repo.Get(ctx, "nonexistent_table")
	if err == nil {
		t.Error("expected error for getting non-existent table, got nil")
	}
}

// TestStorage_RejectsContextCancellation verifies that operations respect
// context cancellation.
//
// Red-Flag: Cancelled operations must fail.
func TestStorage_RejectsContextCancellation(t *testing.T) {
	repo := storage.NewMockRepository()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	table := &tables.VirtualTable{
		Name: "test",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatDelta, Location: "s3://bucket/data"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	}

	err := repo.Create(ctx, table)
	if err == nil {
		t.Error("expected error for cancelled context, got nil")
	}
}

// TestStorage_RejectsEmptyTableName verifies that empty table names in
// Get/Delete are rejected.
//
// Red-Flag: Empty table names must be rejected.
func TestStorage_RejectsEmptyTableName(t *testing.T) {
	repo := storage.NewMockRepository()
	ctx := context.Background()

	t.Run("Get empty name", func(t *testing.T) {
		_, err := repo.Get(ctx, "")
		if err == nil {
			t.Error("expected error for empty table name, got nil")
		}
	})

	t.Run("Delete empty name", func(t *testing.T) {
		err := repo.Delete(ctx, "")
		if err == nil {
			t.Error("expected error for empty table name, got nil")
		}
	})
}

// TestStorage_RejectsContextTimeout verifies that operations respect
// context timeout.
//
// Red-Flag: Timed out operations must fail.
func TestStorage_RejectsContextTimeout(t *testing.T) {
	repo := storage.NewMockRepository()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	// Wait for timeout
	time.Sleep(1 * time.Millisecond)

	_, err := repo.List(ctx)
	if err == nil {
		t.Error("expected error for timed out context, got nil")
	}
}
