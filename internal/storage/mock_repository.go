package storage

import (
	"context"
	"sync"
	"time"

	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/errors"
	"github.com/canonica-labs/canonica/internal/tables"
)

// MockRepository is an in-memory implementation of TableRepository for testing.
// It is thread-safe and respects context cancellation.
// Per phase-3-spec.md §7: In-memory registries may exist ONLY for tests.
type MockRepository struct {
	mu     sync.RWMutex
	tables map[string]*tables.VirtualTable

	// Phase 3: Test helper fields for simulating failures
	connectivityFailure      bool
	persistenceFailure       bool
	connectivityCheckCalled  bool
}

// NewMockRepository creates a new mock repository.
func NewMockRepository() *MockRepository {
	return &MockRepository{
		tables: make(map[string]*tables.VirtualTable),
	}
}

// checkContext verifies the context is not cancelled or timed out.
func checkContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// Create registers a new virtual table.
func (r *MockRepository) Create(ctx context.Context, table *tables.VirtualTable) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	// Validate table definition
	if err := table.Validate(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Phase 3: Simulate persistence failure for testing
	if r.persistenceFailure {
		return errors.NewDatabaseUnavailable("persistence failure (simulated)")
	}

	// Check for duplicate name
	if _, exists := r.tables[table.Name]; exists {
		return errors.NewTableAlreadyExists(table.Name)
	}

	// Set timestamps
	now := time.Now()
	tableCopy := copyTable(table)
	tableCopy.CreatedAt = now
	tableCopy.UpdatedAt = now

	r.tables[table.Name] = tableCopy
	return nil
}

// Get retrieves a virtual table by name.
func (r *MockRepository) Get(ctx context.Context, name string) (*tables.VirtualTable, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}

	if name == "" {
		return nil, errors.NewInvalidTableDefinition("name", "cannot be empty")
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	table, exists := r.tables[name]
	if !exists {
		return nil, errors.NewTableNotFound(name)
	}

	return copyTable(table), nil
}

// Update modifies an existing virtual table.
func (r *MockRepository) Update(ctx context.Context, table *tables.VirtualTable) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	// Validate table definition
	if err := table.Validate(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Check table exists
	existing, exists := r.tables[table.Name]
	if !exists {
		return errors.NewTableNotFound(table.Name)
	}

	// Update with preserved created_at
	tableCopy := copyTable(table)
	tableCopy.CreatedAt = existing.CreatedAt
	tableCopy.UpdatedAt = time.Now()

	r.tables[table.Name] = tableCopy
	return nil
}

// Delete removes a virtual table by name.
func (r *MockRepository) Delete(ctx context.Context, name string) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	if name == "" {
		return errors.NewInvalidTableDefinition("name", "cannot be empty")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.tables[name]; !exists {
		return errors.NewTableNotFound(name)
	}

	delete(r.tables, name)
	return nil
}

// List returns all registered virtual tables.
func (r *MockRepository) List(ctx context.Context) ([]*tables.VirtualTable, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*tables.VirtualTable, 0, len(r.tables))
	for _, table := range r.tables {
		result = append(result, copyTable(table))
	}

	return result, nil
}

// Exists checks if a table with the given name exists.
func (r *MockRepository) Exists(ctx context.Context, name string) (bool, error) {
	if err := checkContext(ctx); err != nil {
		return false, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	_, exists := r.tables[name]
	return exists, nil
}

// copyTable creates a deep copy of a virtual table.
func copyTable(src *tables.VirtualTable) *tables.VirtualTable {
	dst := &tables.VirtualTable{
		Name:        src.Name,
		Description: src.Description,
		CreatedAt:   src.CreatedAt,
		UpdatedAt:   src.UpdatedAt,
	}

	// Copy sources
	if len(src.Sources) > 0 {
		dst.Sources = make([]tables.PhysicalSource, len(src.Sources))
		copy(dst.Sources, src.Sources)
	}

	// Copy capabilities
	if len(src.Capabilities) > 0 {
		dst.Capabilities = make([]capabilities.Capability, len(src.Capabilities))
		copy(dst.Capabilities, src.Capabilities)
	}

	// Copy constraints
	if len(src.Constraints) > 0 {
		dst.Constraints = make([]capabilities.Constraint, len(src.Constraints))
		copy(dst.Constraints, src.Constraints)
	}

	return dst
}

// Phase 3: Test helper methods for simulating failures
// Per phase-3-spec.md §7: These are needed for Red-Flag tests

// SetConnectivityFailure configures the mock to simulate connectivity failures.
func (r *MockRepository) SetConnectivityFailure(fail bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.connectivityFailure = fail
}

// SetPersistenceFailure configures the mock to simulate persistence failures.
func (r *MockRepository) SetPersistenceFailure(fail bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.persistenceFailure = fail
}

// CheckConnectivity verifies database connectivity.
// Per phase-3-spec.md §7: "Add startup checks to verify database connectivity"
func (r *MockRepository) CheckConnectivity(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.connectivityCheckCalled = true

	if r.connectivityFailure {
		return errors.NewDatabaseUnavailable("mock connectivity failure")
	}
	return nil
}

// ConnectivityCheckCalled returns whether CheckConnectivity was called.
func (r *MockRepository) ConnectivityCheckCalled() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.connectivityCheckCalled
}

// Verify MockRepository implements TableRepository interface.
var _ TableRepository = (*MockRepository)(nil)

