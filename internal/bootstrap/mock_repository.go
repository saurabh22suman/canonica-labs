// Package bootstrap provides configuration loading and system initialization.
package bootstrap

import (
	"context"
	"sync"

	"github.com/canonica-labs/canonica/internal/errors"
	"github.com/canonica-labs/canonica/internal/tables"
)

// MockRepository is a test implementation of Repository.
type MockRepository struct {
	mu     sync.RWMutex
	tables map[string]*tables.VirtualTable
}

// NewMockRepository creates a new mock repository.
func NewMockRepository() *MockRepository {
	return &MockRepository{
		tables: make(map[string]*tables.VirtualTable),
	}
}

// Create adds a new table to the repository.
func (r *MockRepository) Create(ctx context.Context, table *tables.VirtualTable) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.tables[table.Name]; exists {
		return errors.NewTableAlreadyExists(table.Name)
	}

	r.tables[table.Name] = table
	return nil
}

// Get retrieves a table by name.
func (r *MockRepository) Get(ctx context.Context, name string) (*tables.VirtualTable, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	table, exists := r.tables[name]
	if !exists {
		return nil, errors.NewTableNotFound(name)
	}

	return table, nil
}

// Update modifies an existing table.
func (r *MockRepository) Update(ctx context.Context, table *tables.VirtualTable) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.tables[table.Name]; !exists {
		return errors.NewTableNotFound(table.Name)
	}

	r.tables[table.Name] = table
	return nil
}

// Delete removes a table by name.
func (r *MockRepository) Delete(ctx context.Context, name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.tables[name]; !exists {
		return errors.NewTableNotFound(name)
	}

	delete(r.tables, name)
	return nil
}

// List returns all tables.
func (r *MockRepository) List(ctx context.Context) ([]*tables.VirtualTable, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*tables.VirtualTable, 0, len(r.tables))
	for _, table := range r.tables {
		result = append(result, table)
	}
	return result, nil
}

// Exists checks if a table exists.
func (r *MockRepository) Exists(ctx context.Context, name string) (bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	_, exists := r.tables[name]
	return exists, nil
}

// TableCount returns the number of tables.
func (r *MockRepository) TableCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.tables)
}

// HasTable checks if a specific table exists.
func (r *MockRepository) HasTable(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, exists := r.tables[name]
	return exists
}
