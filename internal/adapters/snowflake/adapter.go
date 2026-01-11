// Package snowflake provides the Snowflake data warehouse adapter.
//
// Per phase-8-spec.md §4: Connect to Snowflake data warehouse for query execution.
// Per docs/plan.md: "Adapters are stateless, replaceable, thin."
// Per T058: gosnowflake driver integration.
package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/canonica-labs/canonica/internal/adapters"
	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/planner"

	// Import gosnowflake driver - registers as "snowflake"
	_ "github.com/snowflakedb/gosnowflake"
)

// Config configures the Snowflake adapter.
// Per phase-8-spec.md §4.1: Snowflake configuration.
type Config struct {
	// Account is the Snowflake account identifier.
	// Format: <account>.<region>.snowflakecomputing.com
	Account string

	// User is the Snowflake username.
	User string

	// Password for basic auth (or use key-pair).
	Password string

	// PrivateKey for key-pair authentication (PEM format).
	PrivateKey string

	// Database is the default database.
	Database string

	// Schema is the default schema.
	Schema string

	// Warehouse is the compute warehouse.
	Warehouse string

	// Role is the Snowflake role.
	Role string

	// Connection settings
	ConnectTimeout time.Duration
	QueryTimeout   time.Duration
}

// DefaultConfig returns a default configuration.
func DefaultConfig() Config {
	return Config{
		ConnectTimeout: 30 * time.Second,
		QueryTimeout:   5 * time.Minute,
	}
}

// Validate validates the configuration.
func (c Config) Validate() error {
	if c.Account == "" {
		return fmt.Errorf("snowflake: account is required")
	}
	if c.User == "" {
		return fmt.Errorf("snowflake: user is required")
	}
	if c.Password == "" && c.PrivateKey == "" {
		return fmt.Errorf("snowflake: password or private_key is required")
	}
	if c.Warehouse == "" {
		return fmt.Errorf("snowflake: warehouse is required")
	}
	return nil
}

// Adapter implements the EngineAdapter interface for Snowflake.
// Per phase-8-spec.md §4: Snowflake adapter.
type Adapter struct {
	mu     sync.RWMutex
	config Config
	db     *sql.DB
	closed bool
}

// NewAdapter creates a new Snowflake adapter.
// Per phase-8-spec.md §4.1: Returns error if configuration is invalid.
// Per T058: Uses gosnowflake driver for real connectivity.
func NewAdapter(ctx context.Context, config Config) (*Adapter, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	// Build DSN for gosnowflake driver
	// Format: user:password@account/database/schema?warehouse=X&role=Y
	dsn := fmt.Sprintf(
		"%s:%s@%s/%s/%s?warehouse=%s",
		config.User,
		config.Password,
		config.Account,
		config.Database,
		config.Schema,
		config.Warehouse,
	)

	if config.Role != "" {
		dsn += fmt.Sprintf("&role=%s", config.Role)
	}

	// Add connection timeout if specified
	if config.ConnectTimeout > 0 {
		dsn += fmt.Sprintf("&loginTimeout=%d", int(config.ConnectTimeout.Seconds()))
	}

	adapter := &Adapter{
		config: config,
	}

	// Open connection using gosnowflake driver
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return nil, fmt.Errorf("snowflake: failed to open connection: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Test connection with timeout
	pingCtx, cancel := context.WithTimeout(ctx, config.ConnectTimeout)
	defer cancel()

	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, fmt.Errorf("snowflake: connection test failed: %w", err)
	}

	adapter.db = db
	return adapter, nil
}

// NewAdapterWithoutConnect creates a Snowflake adapter without establishing a connection.
// Useful for testing and configuration validation.
// Per T058: Allows adapter creation for unit tests without network access.
func NewAdapterWithoutConnect(config Config) *Adapter {
	return &Adapter{
		config: config,
		db:     nil, // No connection established
		closed: false,
	}
}

// Name returns the adapter name.
func (a *Adapter) Name() string {
	return "snowflake"
}

// Capabilities returns the capabilities this adapter supports.
// Per phase-8-spec.md §4.3: Snowflake Capabilities.
func (a *Adapter) Capabilities() []capabilities.Capability {
	return []capabilities.Capability{
		capabilities.CapabilityRead,
		capabilities.CapabilityAggregate,
		capabilities.CapabilityFilter,
		capabilities.CapabilityTimeTravel, // Snowflake supports up to 90 days
		capabilities.CapabilityWindow,
		capabilities.CapabilityCTE,
	}
}

// Execute runs a query and returns the result.
// Per phase-8-spec.md §4.2: Snowflake Query Execution.
// Per T058: Uses gosnowflake driver for real execution.
func (a *Adapter) Execute(ctx context.Context, plan *planner.ExecutionPlan) (*adapters.QueryResult, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.closed {
		return nil, fmt.Errorf("snowflake: adapter is closed")
	}

	if a.db == nil {
		return nil, fmt.Errorf("snowflake: connection not available")
	}

	if plan == nil || plan.LogicalPlan == nil {
		return nil, fmt.Errorf("snowflake: execution plan is nil")
	}

	sql := plan.LogicalPlan.RawSQL

	// Rewrite time-travel if needed
	if plan.LogicalPlan.HasTimeTravel {
		sql = a.rewriteTimeTravel(sql, plan.LogicalPlan.TimeTravelTimestamp)
	}

	// Execute with timeout
	queryCtx, cancel := context.WithTimeout(ctx, a.config.QueryTimeout)
	defer cancel()

	rows, err := a.db.QueryContext(queryCtx, sql)
	if err != nil {
		return nil, fmt.Errorf("snowflake: query failed: %w", err)
	}
	defer rows.Close()

	return a.collectResults(rows)
}

// rewriteTimeTravel converts time-travel syntax to Snowflake format.
// Per phase-8-spec.md §4.2: Snowflake uses AT(TIMESTAMP => 'ts').
func (a *Adapter) rewriteTimeTravel(sql, timestamp string) string {
	// Replace FOR SYSTEM_TIME AS OF 'ts' with AT(TIMESTAMP => 'ts'::TIMESTAMP)
	oldPattern := fmt.Sprintf("FOR SYSTEM_TIME AS OF '%s'", timestamp)
	newPattern := fmt.Sprintf("AT(TIMESTAMP => '%s'::TIMESTAMP)", timestamp)
	return strings.Replace(sql, oldPattern, newPattern, -1)
}

// collectResults collects query results into a QueryResult.
func (a *Adapter) collectResults(rows *sql.Rows) (*adapters.QueryResult, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("snowflake: failed to get columns: %w", err)
	}

	var resultRows [][]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("snowflake: failed to scan row: %w", err)
		}

		resultRows = append(resultRows, values)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("snowflake: row iteration error: %w", err)
	}

	return &adapters.QueryResult{
		Columns:  columns,
		Rows:     resultRows,
		RowCount: len(resultRows),
		Metadata: map[string]string{
			"engine":    "snowflake",
			"account":   a.config.Account,
			"warehouse": a.config.Warehouse,
		},
	}, nil
}

// Ping checks if Snowflake is reachable.
func (a *Adapter) Ping(ctx context.Context) error {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.closed {
		return fmt.Errorf("snowflake: adapter is closed")
	}

	if a.db == nil {
		return fmt.Errorf("snowflake: driver not available")
	}

	return a.db.PingContext(ctx)
}

// CheckHealth verifies the adapter is healthy.
// Per phase-6-spec.md: Used by /readyz endpoint.
// Per T058: Uses gosnowflake for real health checks.
func (a *Adapter) CheckHealth(ctx context.Context) error {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.closed {
		return fmt.Errorf("snowflake: adapter is closed")
	}

	if a.db == nil {
		return fmt.Errorf("snowflake: connection not available")
	}

	// Execute a simple query to verify connectivity
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var result int
	if err := a.db.QueryRowContext(ctx, "SELECT 1").Scan(&result); err != nil {
		return fmt.Errorf("snowflake: health check failed: %w", err)
	}

	return nil
}

// Close releases resources held by the adapter.
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

// Ensure Adapter implements EngineAdapter interface
var _ adapters.EngineAdapter = (*Adapter)(nil)
