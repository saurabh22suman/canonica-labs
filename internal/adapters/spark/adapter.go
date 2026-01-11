// Package spark provides the Apache Spark engine adapter.
// Spark is the fallback engine per docs/plan.md.
//
// Per docs/plan.md:
//   - "Fallback → Spark"
//   - "Adapters are stateless, replaceable, thin."
//   - "No silent retries. No hidden fallbacks."
//
// Note: Spark Thrift Server uses the HiveServer2 protocol.
// This adapter connects via the standard database/sql interface.
package spark

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/canonica-labs/canonica/internal/adapters"
	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/planner"
)

// Adapter implements the engine adapter interface for Apache Spark.
// Per docs/plan.md: "Fallback → Spark"
//
// Spark SQL is accessed via Spark Thrift Server (HiveServer2 protocol).
// This adapter uses a generic SQL interface that can work with various
// Spark connection methods.
type Adapter struct {
	mu     sync.RWMutex
	db     *sql.DB
	config AdapterConfig
	closed bool
}

// AdapterConfig configures the Spark adapter.
type AdapterConfig struct {
	// Host is the Spark Thrift Server hostname.
	Host string

	// Port is the Spark Thrift Server port (default: 10000).
	Port int

	// Database is the default Spark database/schema.
	Database string

	// User is the user for authentication.
	User string

	// AuthMethod is the authentication method: "NONE", "KERBEROS", etc.
	AuthMethod string

	// ConnectionTimeout for establishing connections.
	ConnectionTimeout time.Duration
}

// NewAdapter creates a new Spark adapter with the given configuration.
func NewAdapter(config AdapterConfig) *Adapter {
	// Apply defaults
	if config.User == "" {
		config.User = "canonica"
	}
	if config.Database == "" {
		config.Database = "default"
	}
	if config.AuthMethod == "" {
		config.AuthMethod = "NONE"
	}
	if config.ConnectionTimeout == 0 {
		config.ConnectionTimeout = 30 * time.Second
	}

	// Note: In production, this would use a Hive/Spark driver.
	// For MVP, we create the adapter structure but defer connection
	// until first use, allowing the adapter to be created without
	// an active Spark cluster.
	return &Adapter{
		config: config,
		closed: false,
	}
}

// Execute runs a query on Spark and returns the result.
// Per docs/plan.md: "Adapters must propagate errors explicitly - never swallow."
func (a *Adapter) Execute(ctx context.Context, plan *planner.ExecutionPlan) (*adapters.QueryResult, error) {
	// Check context first
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("Spark adapter: context error: %w", err)
	}

	// Validate input
	if plan == nil {
		return nil, fmt.Errorf("Spark adapter: execution plan is nil")
	}

	if plan.LogicalPlan == nil {
		return nil, fmt.Errorf("Spark adapter: logical plan is nil")
	}

	if plan.LogicalPlan.RawSQL == "" {
		return nil, fmt.Errorf("Spark adapter: SQL query is empty")
	}

	// Check configuration
	if a.config.Host == "" {
		return nil, fmt.Errorf("Spark adapter: host is not configured")
	}

	// Check if adapter is closed
	a.mu.RLock()
	if a.closed {
		a.mu.RUnlock()
		return nil, fmt.Errorf("Spark adapter: connection is closed")
	}
	a.mu.RUnlock()

	// Attempt to connect and execute
	// Note: In production, this would use an actual Spark/Hive driver.
	// For MVP, we simulate connection attempt to validate connectivity.
	conn, err := a.connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("Spark adapter: connection failed: %w", err)
	}
	defer conn.Close()

	// Execute query
	rows, err := conn.QueryContext(ctx, plan.LogicalPlan.RawSQL)
	if err != nil {
		return nil, fmt.Errorf("Spark adapter: query execution failed: %w", err)
	}
	defer rows.Close()

	// Get column information
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("Spark adapter: failed to get columns: %w", err)
	}

	// Read all rows
	resultRows := make([][]interface{}, 0)
	for rows.Next() {
		// Check context during iteration
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("Spark adapter: context error during row iteration: %w", err)
		}

		// Create slice for row values
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("Spark adapter: failed to scan row: %w", err)
		}

		resultRows = append(resultRows, values)
	}

	// Check for errors during iteration
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("Spark adapter: error during row iteration: %w", err)
	}

	return &adapters.QueryResult{
		Columns:  columns,
		Rows:     resultRows,
		RowCount: len(resultRows),
		Metadata: map[string]string{
			"engine":   "spark",
			"database": a.config.Database,
		},
	}, nil
}

// connect establishes a connection to Spark Thrift Server.
// In MVP, this validates connectivity. Production would use actual driver.
func (a *Adapter) connect(ctx context.Context) (*sql.DB, error) {
	// For MVP, we attempt a TCP connection to verify the server is reachable
	// Production implementation would use:
	// - github.com/apache/hive (Hive driver)
	// - JDBC bridge
	// - Spark Connect (for Spark 3.4+)

	address := fmt.Sprintf("%s:%d", a.config.Host, a.config.Port)

	// Try to establish TCP connection to verify server is reachable
	dialer := &net.Dialer{
		Timeout: a.config.ConnectionTimeout,
	}

	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, fmt.Errorf("cannot reach Spark Thrift Server at %s: %w", address, err)
	}
	conn.Close()

	// Return nil DB since we don't have actual Spark driver in MVP
	// The connection check above validates reachability
	return nil, fmt.Errorf("Spark SQL execution requires Spark Thrift Server driver (not available in MVP)")
}

// Capabilities returns the capabilities this engine supports.
// Per docs/plan.md: Spark is the fallback engine with TIME_TRAVEL support.
func (a *Adapter) Capabilities() []capabilities.Capability {
	return []capabilities.Capability{
		capabilities.CapabilityRead,
		// Spark supports time travel with Delta Lake / Iceberg
		capabilities.CapabilityTimeTravel,
	}
}

// Name returns the engine name.
func (a *Adapter) Name() string {
	return "spark"
}

// Ping checks if Spark Thrift Server is reachable.
func (a *Adapter) Ping(ctx context.Context) error {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.closed {
		return fmt.Errorf("Spark adapter: connection is closed")
	}

	if a.config.Host == "" {
		return fmt.Errorf("Spark adapter: host is not configured")
	}

	// Attempt TCP connection to verify server is reachable
	address := fmt.Sprintf("%s:%d", a.config.Host, a.config.Port)
	dialer := &net.Dialer{
		Timeout: 5 * time.Second,
	}

	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return fmt.Errorf("Spark adapter: cannot reach server at %s: %w", address, err)
	}
	conn.Close()

	return nil
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

// CheckHealth validates Spark Thrift Server connectivity.
// Per phase-6-spec.md: Health check validates server is reachable.
// Since Spark MVP lacks full driver, we verify TCP reachability.
// Returns nil if healthy, error with details if unhealthy.
func (a *Adapter) CheckHealth(ctx context.Context) error {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.closed {
		return fmt.Errorf("Spark adapter: connection is closed")
	}

	if a.config.Host == "" {
		return fmt.Errorf("Spark adapter: host is not configured")
	}

	// Create timeout context for health check
	healthCtx, cancel := context.WithTimeout(ctx, a.config.ConnectionTimeout)
	defer cancel()

	// If we have a database connection, use it for health check
	if a.db != nil {
		var result int
		err := a.db.QueryRowContext(healthCtx, "SELECT 1").Scan(&result)
		if err != nil {
			return fmt.Errorf("Spark adapter health check failed: %w", err)
		}
		if result != 1 {
			return fmt.Errorf("Spark adapter health check: unexpected result %d", result)
		}
		return nil
	}

	// Fall back to TCP connectivity check for MVP
	address := fmt.Sprintf("%s:%d", a.config.Host, a.config.Port)
	dialer := &net.Dialer{
		Timeout: 5 * time.Second,
	}

	conn, err := dialer.DialContext(healthCtx, "tcp", address)
	if err != nil {
		return fmt.Errorf("Spark adapter health check: cannot reach server at %s: %w", address, err)
	}
	conn.Close()

	return nil
}
