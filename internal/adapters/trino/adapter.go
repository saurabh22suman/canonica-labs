// Package trino provides the Trino engine adapter.
// Trino is the primary read engine per docs/plan.md.
//
// Per docs/plan.md: "Adapters are stateless, replaceable, thin.
// No silent retries. No hidden fallbacks."
package trino

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/canonica-labs/canonica/internal/adapters"
	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/planner"

	_ "github.com/trinodb/trino-go-client/trino" // Trino driver
)

// Adapter implements the engine adapter interface for Trino.
// Per docs/plan.md: "Trino (primary read engine)"
type Adapter struct {
	mu     sync.RWMutex
	db     *sql.DB
	config AdapterConfig
	closed bool
}

// AdapterConfig configures the Trino adapter.
type AdapterConfig struct {
	// Host is the Trino coordinator hostname.
	Host string

	// Port is the Trino coordinator port.
	Port int

	// Catalog is the default Trino catalog.
	Catalog string

	// Schema is the default Trino schema.
	Schema string

	// User is the Trino user for queries.
	User string

	// SSLMode controls SSL/TLS: "", "disable", "require"
	SSLMode string

	// Connection pool settings per phase-6-spec.md
	// MaxOpenConns is the maximum number of open connections. Default: 10.
	MaxOpenConns int

	// MaxIdleConns is the maximum number of idle connections. Default: 5.
	MaxIdleConns int

	// ConnMaxLifetime is the maximum lifetime of a connection. Default: 5 minutes.
	ConnMaxLifetime time.Duration

	// ConnMaxIdleTime is the maximum idle time of a connection. Default: 1 minute.
	ConnMaxIdleTime time.Duration

	// ConnectTimeout is the timeout for establishing connections. Default: 10 seconds.
	ConnectTimeout time.Duration

	// QueryTimeout is the default query timeout. Default: 5 minutes.
	QueryTimeout time.Duration
}

// NewAdapter creates a new Trino adapter with the given configuration.
// Per phase-6-spec.md: Configures connection pooling and validates settings.
func NewAdapter(config AdapterConfig) *Adapter {
	// Apply defaults
	if config.User == "" {
		config.User = "canonica"
	}
	if config.Catalog == "" {
		config.Catalog = "memory"
	}
	if config.Schema == "" {
		config.Schema = "default"
	}

	// Apply connection pool defaults per phase-6-spec.md
	if config.MaxOpenConns <= 0 {
		config.MaxOpenConns = 10
	}
	if config.MaxIdleConns <= 0 {
		config.MaxIdleConns = 5
	}
	if config.ConnMaxLifetime <= 0 {
		config.ConnMaxLifetime = 5 * time.Minute
	}
	if config.ConnMaxIdleTime <= 0 {
		config.ConnMaxIdleTime = 1 * time.Minute
	}
	if config.ConnectTimeout <= 0 {
		config.ConnectTimeout = 10 * time.Second
	}
	if config.QueryTimeout <= 0 {
		config.QueryTimeout = 5 * time.Minute
	}

	// Build DSN
	// Format: http[s]://user@host:port?catalog=X&schema=Y
	scheme := "http"
	if config.SSLMode == "require" {
		scheme = "https"
	}

	dsn := fmt.Sprintf("%s://%s@%s:%d?catalog=%s&schema=%s",
		scheme,
		config.User,
		config.Host,
		config.Port,
		config.Catalog,
		config.Schema,
	)

	// Open database connection
	db, err := sql.Open("trino", dsn)
	if err != nil {
		// Return adapter in failed state - will error on first use
		return &Adapter{
			config: config,
			closed: true,
		}
	}

	// Configure connection pool per phase-6-spec.md
	db.SetMaxOpenConns(config.MaxOpenConns)
	db.SetMaxIdleConns(config.MaxIdleConns)
	db.SetConnMaxLifetime(config.ConnMaxLifetime)
	db.SetConnMaxIdleTime(config.ConnMaxIdleTime)

	return &Adapter{
		db:     db,
		config: config,
		closed: false,
	}
}

// Execute runs a query on Trino and returns the result.
// Per docs/plan.md: "Adapters must propagate errors explicitly - never swallow."
func (a *Adapter) Execute(ctx context.Context, plan *planner.ExecutionPlan) (*adapters.QueryResult, error) {
	// Check context first
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("Trino adapter: context error: %w", err)
	}

	// Validate input
	if plan == nil {
		return nil, fmt.Errorf("Trino adapter: execution plan is nil")
	}

	if plan.LogicalPlan == nil {
		return nil, fmt.Errorf("Trino adapter: logical plan is nil")
	}

	if plan.LogicalPlan.RawSQL == "" {
		return nil, fmt.Errorf("Trino adapter: SQL query is empty")
	}

	// Check configuration
	if a.config.Host == "" {
		return nil, fmt.Errorf("Trino adapter: host is not configured")
	}

	// Check if adapter is closed
	a.mu.RLock()
	if a.closed || a.db == nil {
		a.mu.RUnlock()
		return nil, fmt.Errorf("Trino adapter: connection is closed")
	}
	db := a.db
	a.mu.RUnlock()

	// Execute query with context
	rows, err := db.QueryContext(ctx, plan.LogicalPlan.RawSQL)
	if err != nil {
		return nil, fmt.Errorf("Trino adapter: query execution failed: %w", err)
	}
	defer rows.Close()

	// Get column information
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("Trino adapter: failed to get columns: %w", err)
	}

	// Read all rows
	resultRows := make([][]interface{}, 0)
	for rows.Next() {
		// Check context during iteration
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("Trino adapter: context error during row iteration: %w", err)
		}

		// Create slice for row values
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("Trino adapter: failed to scan row: %w", err)
		}

		resultRows = append(resultRows, values)
	}

	// Check for errors during iteration
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("Trino adapter: error during row iteration: %w", err)
	}

	return &adapters.QueryResult{
		Columns:  columns,
		Rows:     resultRows,
		RowCount: len(resultRows),
		Metadata: map[string]string{
			"engine":  "trino",
			"catalog": a.config.Catalog,
			"schema":  a.config.Schema,
		},
	}, nil
}

// Capabilities returns the capabilities this engine supports.
// Per docs/plan.md: "Trino (primary read engine)"
func (a *Adapter) Capabilities() []capabilities.Capability {
	return []capabilities.Capability{
		capabilities.CapabilityRead,
		// Note: Time travel support depends on connector (e.g., Iceberg)
		// We report it as available since Trino supports it with compatible tables
		capabilities.CapabilityTimeTravel,
	}
}

// Name returns the engine name.
func (a *Adapter) Name() string {
	return "trino"
}

// Ping checks if Trino is reachable.
func (a *Adapter) Ping(ctx context.Context) error {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.closed || a.db == nil {
		return fmt.Errorf("Trino adapter: connection is closed")
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

// CheckHealth validates the connection by executing SELECT 1.
// Per phase-6-spec.md: Health check uses simple query validation.
// Returns nil if healthy, error with details if unhealthy.
func (a *Adapter) CheckHealth(ctx context.Context) error {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.closed {
		return fmt.Errorf("Trino adapter: connection is closed")
	}

	if a.db == nil {
		return fmt.Errorf("Trino adapter: no database connection")
	}

	// Create timeout context for health check
	healthCtx, cancel := context.WithTimeout(ctx, a.config.ConnectTimeout)
	defer cancel()

	// Execute SELECT 1 as health check
	var result int
	err := a.db.QueryRowContext(healthCtx, "SELECT 1").Scan(&result)
	if err != nil {
		return fmt.Errorf("Trino adapter health check failed: %w", err)
	}

	if result != 1 {
		return fmt.Errorf("Trino adapter health check: unexpected result %d", result)
	}

	return nil
}
