// Package adapters defines the common interface for engine adapters.
// Each adapter translates between the planner's execution plan and
// the engine's native query format.
//
// Per docs/plan.md: "Adapters are stateless, replaceable, thin.
// No silent retries. No hidden fallbacks."
package adapters

import (
	"context"

	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/planner"
)

// QueryResult represents the result of a query execution.
type QueryResult struct {
	// Columns are the column names in the result.
	Columns []string

	// Rows are the result rows, each row is a slice of values.
	Rows [][]interface{}

	// RowCount is the number of rows returned.
	RowCount int

	// Metadata contains additional execution information.
	Metadata map[string]string
}

// EngineAdapter is the interface all engine adapters must implement.
// Adapters must be:
// - Stateless: Each operation is independent
// - Thin: Minimal logic, just translation
// - Explicit: No silent retries, no hidden fallbacks
type EngineAdapter interface {
	// Name returns the unique name of this engine.
	Name() string

	// Capabilities returns the capabilities this engine supports.
	Capabilities() []capabilities.Capability

	// Execute runs a query and returns the result.
	// Must propagate errors explicitly - never swallow.
	Execute(ctx context.Context, plan *planner.ExecutionPlan) (*QueryResult, error)

	// Ping checks if the engine is reachable.
	Ping(ctx context.Context) error

	// CheckHealth verifies the adapter is healthy and can execute queries.
	// Per phase-6-spec.md: Returns nil if healthy, error with details if not.
	// Used by /readyz endpoint to report per-adapter health status.
	CheckHealth(ctx context.Context) error

	// Close releases any resources held by the adapter.
	Close() error
}

// AdapterRegistry manages engine adapters.
type AdapterRegistry struct {
	adapters map[string]EngineAdapter
}

// NewAdapterRegistry creates a new adapter registry.
func NewAdapterRegistry() *AdapterRegistry {
	return &AdapterRegistry{
		adapters: make(map[string]EngineAdapter),
	}
}

// Register adds an adapter to the registry.
func (r *AdapterRegistry) Register(adapter EngineAdapter) {
	r.adapters[adapter.Name()] = adapter
}

// Get returns an adapter by name.
func (r *AdapterRegistry) Get(name string) (EngineAdapter, bool) {
	adapter, ok := r.adapters[name]
	return adapter, ok
}

// Available returns the names of all registered adapters.
func (r *AdapterRegistry) Available() []string {
	names := make([]string, 0, len(r.adapters))
	for name := range r.adapters {
		names = append(names, name)
	}
	return names
}

// CloseAll closes all registered adapters.
func (r *AdapterRegistry) CloseAll() error {
	var lastErr error
	for _, adapter := range r.adapters {
		if err := adapter.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// CheckAllHealth checks the health of all registered adapters.
// Per phase-6-spec.md: Returns a map of adapter name to health status.
// A nil error value indicates the adapter is healthy.
func (r *AdapterRegistry) CheckAllHealth(ctx context.Context) map[string]error {
	results := make(map[string]error)
	for name, adapter := range r.adapters {
		results[name] = adapter.CheckHealth(ctx)
	}
	return results
}

// IsEmpty returns true if no adapters are registered.
func (r *AdapterRegistry) IsEmpty() bool {
	return len(r.adapters) == 0
}
