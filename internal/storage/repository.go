// Package storage provides persistence for the canonica control plane.
// This includes the TableRepository for virtual table CRUD operations.
//
// Per docs/plan.md: "PostgreSQL for virtual tables, capabilities, constraints, routing rules, audit logs."
// Per execution-checklist.md: "Planner reads tables, roles, constraints **only** from repository"
package storage

import (
	"context"

	"github.com/canonica-labs/canonica/internal/errors"
	"github.com/canonica-labs/canonica/internal/tables"
)

// TableRepository defines the interface for virtual table persistence.
// All implementations must be:
// - Thread-safe
// - Context-aware (respecting cancellation/timeout)
// - Explicit about errors (never swallow)
//
// Per execution-checklist.md 4.1: Repository is mandatory in gateway constructor.
// The Gateway and Planner MUST use this interface for all table operations.
type TableRepository interface {
	// Create registers a new virtual table.
	// Returns an error if:
	// - Table name already exists
	// - Table definition is invalid
	// - Context is cancelled
	Create(ctx context.Context, table *tables.VirtualTable) error

	// Get retrieves a virtual table by name.
	// Returns an error if:
	// - Table does not exist
	// - Context is cancelled
	Get(ctx context.Context, name string) (*tables.VirtualTable, error)

	// Update modifies an existing virtual table.
	// Returns an error if:
	// - Table does not exist
	// - Table definition is invalid
	// - Context is cancelled
	Update(ctx context.Context, table *tables.VirtualTable) error

	// Delete removes a virtual table by name.
	// Returns an error if:
	// - Table does not exist
	// - Context is cancelled
	Delete(ctx context.Context, name string) error

	// List returns all registered virtual tables.
	// Returns empty slice (not nil) if no tables exist.
	// Returns an error if:
	// - Context is cancelled
	List(ctx context.Context) ([]*tables.VirtualTable, error)

	// Exists checks if a table with the given name exists.
	// Returns an error if:
	// - Context is cancelled
	Exists(ctx context.Context, name string) (bool, error)

	// CheckConnectivity verifies database connectivity.
	// Per phase-3-spec.md §7: "Add startup checks to verify database connectivity"
	// Per execution-checklist.md 4.1: "Gateway startup fails if PostgreSQL is unavailable"
	CheckConnectivity(ctx context.Context) error
}

// DetectMetadataConflict checks if two repositories have conflicting definitions for a table.
// Per phase-3-spec.md §7: "Two conflicting metadata sources detected → must fail"
func DetectMetadataConflict(ctx context.Context, tableName string, primary, secondary TableRepository) error {
	primaryTable, err1 := primary.Get(ctx, tableName)
	secondaryTable, err2 := secondary.Get(ctx, tableName)

	// If table doesn't exist in one or both, no conflict
	if err1 != nil || err2 != nil {
		return nil
	}

	// Compare definitions - they must match
	if !tablesMatch(primaryTable, secondaryTable) {
		return errors.NewMetadataConflict(tableName, "primary", "secondary")
	}

	return nil
}

// tablesMatch checks if two virtual tables have the same definition.
func tablesMatch(a, b *tables.VirtualTable) bool {
	if a.Name != b.Name {
		return false
	}

	// Compare sources
	if len(a.Sources) != len(b.Sources) {
		return false
	}
	for i := range a.Sources {
		if a.Sources[i].Format != b.Sources[i].Format ||
			a.Sources[i].Location != b.Sources[i].Location {
			return false
		}
	}

	// Compare capabilities
	if len(a.Capabilities) != len(b.Capabilities) {
		return false
	}
	for i := range a.Capabilities {
		if a.Capabilities[i] != b.Capabilities[i] {
			return false
		}
	}

	return true
}
