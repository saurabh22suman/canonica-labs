// Package bigquery provides the Google BigQuery data warehouse adapter.
//
// Per phase-8-spec.md §5: Connect to Google BigQuery for query execution.
// Per docs/plan.md: "Adapters are stateless, replaceable, thin."
// Per T059: BigQuery SDK integration.
package bigquery

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/canonica-labs/canonica/internal/adapters"
	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/planner"
)

// Config configures the BigQuery adapter.
// Per phase-8-spec.md §5.1: BigQuery configuration.
type Config struct {
	// ProjectID is the GCP project ID.
	ProjectID string

	// CredentialsJSON is the service account key (optional if using ADC).
	CredentialsJSON string

	// Location is the BigQuery region (e.g., "US", "EU").
	Location string

	// DefaultDataset is the default dataset for unqualified tables.
	DefaultDataset string

	// QueryTimeout for query execution.
	QueryTimeout time.Duration
}

// DefaultConfig returns a default configuration.
func DefaultConfig() Config {
	return Config{
		Location:     "US",
		QueryTimeout: 5 * time.Minute,
	}
}

// Validate validates the configuration.
func (c Config) Validate() error {
	if c.ProjectID == "" {
		return fmt.Errorf("bigquery: project_id is required")
	}
	return nil
}

// Adapter implements the EngineAdapter interface for BigQuery.
// Per phase-8-spec.md §5: BigQuery adapter.
// Per T059: Uses cloud.google.com/go/bigquery SDK.
type Adapter struct {
	mu     sync.RWMutex
	config Config
	client *bigquery.Client
	closed bool
}

// NewAdapter creates a new BigQuery adapter.
// Per phase-8-spec.md §5.1: Returns error if configuration is invalid.
// Per T059: Uses Google Cloud SDK for real connectivity.
func NewAdapter(ctx context.Context, config Config) (*Adapter, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	// Build client options
	var opts []option.ClientOption
	if config.CredentialsJSON != "" {
		opts = append(opts, option.WithCredentialsJSON([]byte(config.CredentialsJSON)))
	}
	// If no credentials provided, SDK will use Application Default Credentials (ADC)

	// Create BigQuery client
	client, err := bigquery.NewClient(ctx, config.ProjectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("bigquery: failed to create client: %w", err)
	}

	adapter := &Adapter{
		config: config,
		client: client,
	}

	return adapter, nil
}

// NewAdapterWithoutConnect creates a BigQuery adapter without establishing a connection.
// Useful for testing and configuration validation.
// Per T059: Allows adapter creation for unit tests without network access.
func NewAdapterWithoutConnect(config Config) *Adapter {
	return &Adapter{
		config: config,
		client: nil, // No connection established
		closed: false,
	}
}

// Name returns the adapter name.
func (a *Adapter) Name() string {
	return "bigquery"
}

// Capabilities returns the capabilities this adapter supports.
// Per phase-8-spec.md §5: BigQuery capabilities.
func (a *Adapter) Capabilities() []capabilities.Capability {
	return []capabilities.Capability{
		capabilities.CapabilityRead,
		capabilities.CapabilityAggregate,
		capabilities.CapabilityFilter,
		capabilities.CapabilityTimeTravel, // BigQuery supports up to 7 days
		capabilities.CapabilityWindow,
		capabilities.CapabilityCTE,
	}
}

// Execute runs a query and returns the result.
// Per phase-8-spec.md §5.2: BigQuery Query Execution.
// Per T059: Uses BigQuery SDK for real execution.
func (a *Adapter) Execute(ctx context.Context, plan *planner.ExecutionPlan) (*adapters.QueryResult, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.closed {
		return nil, fmt.Errorf("bigquery: adapter is closed")
	}

	if a.client == nil {
		return nil, fmt.Errorf("bigquery: client not available")
	}

	if plan == nil || plan.LogicalPlan == nil {
		return nil, fmt.Errorf("bigquery: execution plan is nil")
	}

	sql := plan.LogicalPlan.RawSQL

	// Rewrite time-travel if needed
	if plan.LogicalPlan.HasTimeTravel {
		sql = a.rewriteTimeTravel(sql, plan.LogicalPlan.TimeTravelTimestamp)
	}

	// Create query with timeout
	queryCtx, cancel := context.WithTimeout(ctx, a.config.QueryTimeout)
	defer cancel()

	q := a.client.Query(sql)
	if a.config.DefaultDataset != "" {
		q.DefaultDatasetID = a.config.DefaultDataset
	}
	if a.config.Location != "" {
		q.Location = a.config.Location
	}

	// Run query
	it, err := q.Read(queryCtx)
	if err != nil {
		return nil, fmt.Errorf("bigquery: query failed: %w", err)
	}

	return a.collectResults(it)
}

// collectResults collects BigQuery results into a QueryResult.
func (a *Adapter) collectResults(it *bigquery.RowIterator) (*adapters.QueryResult, error) {
	// Get schema for column names
	schema := it.Schema
	columns := make([]string, len(schema))
	for i, field := range schema {
		columns[i] = field.Name
	}

	var resultRows [][]interface{}
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("bigquery: failed to read row: %w", err)
		}

		// Convert BigQuery values to interface{}
		rowData := make([]interface{}, len(row))
		for i, v := range row {
			rowData[i] = v
		}
		resultRows = append(resultRows, rowData)
	}

	return &adapters.QueryResult{
		Columns:  columns,
		Rows:     resultRows,
		RowCount: len(resultRows),
		Metadata: map[string]string{
			"engine":    "bigquery",
			"project":   a.config.ProjectID,
			"location":  a.config.Location,
		},
	}, nil
}

// rewriteTimeTravel converts time-travel syntax to BigQuery format.
// Per phase-8-spec.md §5.2: BigQuery uses similar syntax to Canonic.
func (a *Adapter) rewriteTimeTravel(sql, timestamp string) string {
	// BigQuery expects: FOR SYSTEM_TIME AS OF TIMESTAMP 'ts'
	// Our syntax: FOR SYSTEM_TIME AS OF 'ts'
	oldPattern := fmt.Sprintf("FOR SYSTEM_TIME AS OF '%s'", timestamp)
	newPattern := fmt.Sprintf("FOR SYSTEM_TIME AS OF TIMESTAMP '%s'", timestamp)
	return strings.Replace(sql, oldPattern, newPattern, -1)
}

// Ping checks if BigQuery is reachable.
// Per T059: Uses BigQuery SDK for connectivity check.
func (a *Adapter) Ping(ctx context.Context) error {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.closed {
		return fmt.Errorf("bigquery: adapter is closed")
	}

	if a.client == nil {
		return fmt.Errorf("bigquery: client not available")
	}

	// Execute a simple query to verify connectivity
	q := a.client.Query("SELECT 1")
	_, err := q.Read(ctx)
	if err != nil {
		return fmt.Errorf("bigquery: ping failed: %w", err)
	}

	return nil
}

// CheckHealth verifies the adapter is healthy.
// Per phase-6-spec.md: Used by /readyz endpoint.
// Per T059: Uses BigQuery SDK for health checks.
func (a *Adapter) CheckHealth(ctx context.Context) error {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.closed {
		return fmt.Errorf("bigquery: adapter is closed")
	}

	if a.client == nil {
		return fmt.Errorf("bigquery: client not available")
	}

	// Execute a simple query with timeout
	healthCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	q := a.client.Query("SELECT 1")
	it, err := q.Read(healthCtx)
	if err != nil {
		return fmt.Errorf("bigquery: health check failed: %w", err)
	}

	// Read at least one row to verify full connectivity
	var row []bigquery.Value
	if err := it.Next(&row); err != nil && err != iterator.Done {
		return fmt.Errorf("bigquery: health check read failed: %w", err)
	}

	return nil
}

// Close releases resources held by the adapter.
// Per T059: Properly closes BigQuery client.
func (a *Adapter) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return nil
	}

	a.closed = true
	if a.client != nil {
		return a.client.Close()
	}
	return nil
}

// Ensure Adapter implements EngineAdapter interface
var _ adapters.EngineAdapter = (*Adapter)(nil)
