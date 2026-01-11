// Package redshift provides the Amazon Redshift data warehouse adapter.
//
// Per phase-8-spec.md §6: Connect to Amazon Redshift for query execution.
// Per docs/plan.md: "Adapters are stateless, replaceable, thin."
package redshift

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/canonica-labs/canonica/internal/adapters"
	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/planner"

	// Import postgres driver for Redshift (uses postgres protocol)
	_ "github.com/lib/pq"
)

// Config configures the Redshift adapter.
// Per phase-8-spec.md §6.1: Redshift configuration.
type Config struct {
	// Host is the Redshift cluster endpoint.
	Host string

	// Port is the Redshift port (default 5439).
	Port int

	// Database is the Redshift database name.
	Database string

	// User is the database user.
	User string

	// Password is the database password.
	Password string

	// SSLMode controls SSL: disable, require, verify-ca, verify-full
	SSLMode string

	// IAM Auth (alternative to password)
	UseIAMAuth bool
	AWSRegion  string
	ClusterID  string

	// Connection settings
	ConnectTimeout time.Duration
	QueryTimeout   time.Duration
}

// DefaultConfig returns a default configuration.
func DefaultConfig() Config {
	return Config{
		Port:           5439,
		SSLMode:        "require",
		ConnectTimeout: 30 * time.Second,
		QueryTimeout:   5 * time.Minute,
	}
}

// Validate validates the configuration.
func (c Config) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("redshift: host is required")
	}
	if c.Database == "" {
		return fmt.Errorf("redshift: database is required")
	}
	if c.User == "" {
		return fmt.Errorf("redshift: user is required")
	}
	if !c.UseIAMAuth && c.Password == "" {
		return fmt.Errorf("redshift: password is required when not using IAM auth")
	}
	if c.UseIAMAuth && (c.AWSRegion == "" || c.ClusterID == "") {
		return fmt.Errorf("redshift: aws_region and cluster_id required for IAM auth")
	}
	return nil
}

// Adapter implements the EngineAdapter interface for Redshift.
// Per phase-8-spec.md §6: Redshift adapter.
type Adapter struct {
	mu     sync.RWMutex
	config Config
	db     *sql.DB
	closed bool
}

// NewAdapter creates a new Redshift adapter.
// Per phase-8-spec.md §6.1: Returns error if configuration is invalid.
func NewAdapter(ctx context.Context, config Config) (*Adapter, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	// Build DSN for postgres driver
	var dsn string

	if config.UseIAMAuth {
		// IAM authentication requires AWS SDK to get temporary credentials
		// For MVP, we return an error indicating IAM auth requires additional setup
		return nil, fmt.Errorf(
			"redshift: IAM authentication requires AWS SDK; " +
				"use password authentication or add github.com/aws/aws-sdk-go-v2")
	}

	dsn = fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		config.Host, config.Port, config.Database,
		config.User, config.Password, config.SSLMode,
	)

	// Open connection
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("redshift: failed to connect: %w", err)
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
		return nil, fmt.Errorf("redshift: connection test failed: %w", err)
	}

	return &Adapter{
		config: config,
		db:     db,
	}, nil
}

// Name returns the adapter name.
func (a *Adapter) Name() string {
	return "redshift"
}

// Capabilities returns the capabilities this adapter supports.
// Per phase-8-spec.md §6.2: Redshift does NOT support time-travel.
func (a *Adapter) Capabilities() []capabilities.Capability {
	return []capabilities.Capability{
		capabilities.CapabilityRead,
		capabilities.CapabilityAggregate,
		capabilities.CapabilityFilter,
		capabilities.CapabilityWindow,
		// NO CapabilityTimeTravel - Redshift doesn't support it
	}
}

// Execute runs a query and returns the result.
// Per phase-8-spec.md §6.2: Redshift Limitations - no time-travel.
func (a *Adapter) Execute(ctx context.Context, plan *planner.ExecutionPlan) (*adapters.QueryResult, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.closed {
		return nil, fmt.Errorf("redshift: adapter is closed")
	}

	if plan == nil || plan.LogicalPlan == nil {
		return nil, fmt.Errorf("redshift: execution plan is nil")
	}

	// Per phase-8-spec.md §6.2: Redshift does not support time-travel
	if plan.LogicalPlan.HasTimeTravel {
		return nil, fmt.Errorf(
			"redshift: time-travel queries are not supported; " +
				"Redshift does not have built-in time-travel capability")
	}

	sql := plan.LogicalPlan.RawSQL

	// Execute with timeout
	queryCtx, cancel := context.WithTimeout(ctx, a.config.QueryTimeout)
	defer cancel()

	rows, err := a.db.QueryContext(queryCtx, sql)
	if err != nil {
		return nil, fmt.Errorf("redshift: query failed: %w", err)
	}
	defer rows.Close()

	return a.collectResults(rows)
}

// collectResults collects query results into a QueryResult.
func (a *Adapter) collectResults(rows *sql.Rows) (*adapters.QueryResult, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("redshift: failed to get columns: %w", err)
	}

	var resultRows [][]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("redshift: failed to scan row: %w", err)
		}

		resultRows = append(resultRows, values)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("redshift: row iteration error: %w", err)
	}

	return &adapters.QueryResult{
		Columns:  columns,
		Rows:     resultRows,
		RowCount: len(resultRows),
		Metadata: map[string]string{
			"engine":   "redshift",
			"host":     a.config.Host,
			"database": a.config.Database,
		},
	}, nil
}

// Ping checks if Redshift is reachable.
func (a *Adapter) Ping(ctx context.Context) error {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.closed {
		return fmt.Errorf("redshift: adapter is closed")
	}

	return a.db.PingContext(ctx)
}

// CheckHealth verifies the adapter is healthy.
// Per phase-6-spec.md: Used by /readyz endpoint.
func (a *Adapter) CheckHealth(ctx context.Context) error {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.closed {
		return fmt.Errorf("redshift: adapter is closed")
	}

	// Execute a simple query to verify connectivity
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var result int
	if err := a.db.QueryRowContext(ctx, "SELECT 1").Scan(&result); err != nil {
		return fmt.Errorf("redshift: health check failed: %w", err)
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
