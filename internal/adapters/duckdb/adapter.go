// Package duckdb provides the DuckDB engine adapter.
// DuckDB is used for local development and as the MVP engine.
//
// Per docs/plan.md: "Adapters are stateless, replaceable, thin."
package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/canonica-labs/canonica/internal/adapters"
	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/planner"

	_ "github.com/marcboeker/go-duckdb" // DuckDB driver
)

// Adapter implements the engine adapter interface for DuckDB.
// The adapter maintains a connection pool for query execution.
type Adapter struct {
	mu               sync.RWMutex
	db               *sql.DB
	connectionString string
	closed           bool
}

// AdapterConfig configures the DuckDB adapter.
type AdapterConfig struct {
	// DatabasePath is the path to the DuckDB database file.
	// Use ":memory:" for in-memory database.
	DatabasePath string
}

// NewAdapter creates a new DuckDB adapter with default in-memory configuration.
func NewAdapter() *Adapter {
	return NewAdapterWithConfig(AdapterConfig{DatabasePath: ":memory:"})
}

// NewAdapterWithConfig creates a new DuckDB adapter with the given configuration.
func NewAdapterWithConfig(config AdapterConfig) *Adapter {
	connStr := config.DatabasePath
	if connStr == "" {
		connStr = ":memory:"
	}

	// Open DuckDB connection
	db, err := sql.Open("duckdb", connStr)
	if err != nil {
		// Return adapter in failed state - will error on first use
		return &Adapter{
			connectionString: connStr,
			closed:           true,
		}
	}

	return &Adapter{
		db:               db,
		connectionString: connStr,
		closed:           false,
	}
}

// Execute runs a query on DuckDB and returns the result.
// Per docs/plan.md: "Adapters must propagate errors explicitly - never swallow."
func (a *Adapter) Execute(ctx context.Context, plan *planner.ExecutionPlan) (*adapters.QueryResult, error) {
	// Check context first
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("DuckDB adapter: context error: %w", err)
	}

	// Validate input
	if plan == nil {
		return nil, fmt.Errorf("DuckDB adapter: execution plan is nil")
	}

	if plan.LogicalPlan == nil {
		return nil, fmt.Errorf("DuckDB adapter: logical plan is nil")
	}

	if plan.LogicalPlan.RawSQL == "" {
		return nil, fmt.Errorf("DuckDB adapter: SQL query is empty")
	}

	// Check if adapter is closed
	a.mu.RLock()
	if a.closed || a.db == nil {
		a.mu.RUnlock()
		return nil, fmt.Errorf("DuckDB adapter: connection is closed")
	}
	db := a.db
	a.mu.RUnlock()

	// Execute query with context
	rows, err := db.QueryContext(ctx, plan.LogicalPlan.RawSQL)
	if err != nil {
		return nil, fmt.Errorf("DuckDB adapter: query execution failed: %w", err)
	}
	defer rows.Close()

	// Get column information
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("DuckDB adapter: failed to get columns: %w", err)
	}

	// Read all rows
	resultRows := make([][]interface{}, 0)
	for rows.Next() {
		// Check context during iteration
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("DuckDB adapter: context error during row iteration: %w", err)
		}

		// Create slice for row values
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("DuckDB adapter: failed to scan row: %w", err)
		}

		resultRows = append(resultRows, values)
	}

	// Check for errors during iteration
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("DuckDB adapter: error during row iteration: %w", err)
	}

	return &adapters.QueryResult{
		Columns:  columns,
		Rows:     resultRows,
		RowCount: len(resultRows),
		Metadata: map[string]string{
			"engine": "duckdb",
		},
	}, nil
}

// Capabilities returns the capabilities this engine supports.
func (a *Adapter) Capabilities() []capabilities.Capability {
	return []capabilities.Capability{
		capabilities.CapabilityRead,
		capabilities.CapabilityTimeTravel,
	}
}

// Name returns the engine name.
func (a *Adapter) Name() string {
	return "duckdb"
}

// Ping checks if the engine is reachable.
func (a *Adapter) Ping(ctx context.Context) error {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.closed || a.db == nil {
		return fmt.Errorf("DuckDB adapter: connection is closed")
	}

	return a.db.PingContext(ctx)
}

// Close releases any resources held by the adapter.
// Close is idempotent - safe to call multiple times.
func (a *Adapter) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return nil
	}

	a.closed = true

	if a.db != nil {
		return a.db.Close()
	}

	return nil
}
