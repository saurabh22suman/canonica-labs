// Package glue provides the AWS Glue Data Catalog client.
//
// Per phase-7-spec.md §3: Connect to AWS Glue Data Catalog to discover tables.
// Per docs/plan.md: "Adapters are stateless, replaceable, thin."
package glue

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/canonica-labs/canonica/internal/catalog"
)

// Config configures the AWS Glue client.
// Per phase-7-spec.md §3.1: AWS Glue configuration.
type Config struct {
	// Region is the AWS region.
	Region string

	// CatalogID is the AWS account ID (optional, uses caller's account if empty).
	CatalogID string

	// Credentials (if not using IAM role/instance profile)
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string

	// RequestTimeout for API calls.
	RequestTimeout time.Duration

	// IncludeDatabases filters which databases to sync (empty = all).
	IncludeDatabases []string

	// ExcludeDatabases filters which databases to exclude.
	ExcludeDatabases []string
}

// DefaultConfig returns a default configuration.
func DefaultConfig() Config {
	return Config{
		RequestTimeout: 30 * time.Second,
	}
}

// Validate validates the configuration.
func (c Config) Validate() error {
	if c.Region == "" {
		return fmt.Errorf("glue: region is required")
	}

	// Validate region format (basic check)
	if !isValidAWSRegion(c.Region) {
		return fmt.Errorf("glue: invalid region format %q", c.Region)
	}

	return nil
}

// isValidAWSRegion performs basic validation of AWS region format.
func isValidAWSRegion(region string) bool {
	// Basic pattern: letters-letters-number
	// e.g., us-east-1, eu-west-2, ap-southeast-1
	parts := strings.Split(region, "-")
	if len(parts) < 3 {
		return false
	}
	return true
}

// Client implements the Catalog interface for AWS Glue.
// Per phase-7-spec.md §3: AWS Glue Integration.
type Client struct {
	mu     sync.RWMutex
	config Config
	closed bool

	// Note: In production, this would use AWS SDK Glue client.
	// For MVP, we simulate the interface with error handling.
}

// NewClient creates a new AWS Glue client.
// Per phase-7-spec.md §3.1: Returns error if configuration is invalid.
func NewClient(ctx context.Context, config Config) (*Client, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	// Apply defaults
	if config.RequestTimeout <= 0 {
		config.RequestTimeout = 30 * time.Second
	}

	return &Client{
		config: config,
		closed: false,
	}, nil
}

// Name returns the catalog identifier.
func (c *Client) Name() string {
	return "glue"
}

// CheckConnectivity verifies AWS Glue is reachable.
// Per phase-7-spec.md: Health check validates AWS credentials and access.
func (c *Client) CheckConnectivity(ctx context.Context) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return fmt.Errorf("glue: client is closed")
	}

	// Note: In production, this would verify AWS credentials by making
	// a simple API call like GetDatabases with limit 1.
	//
	// For MVP without AWS SDK dependency, we return an error indicating
	// the SDK is required.
	return fmt.Errorf("glue: AWS SDK not implemented; " +
		"requires github.com/aws/aws-sdk-go-v2 dependency; " +
		"see phase-7-spec.md Appendix B for required dependencies")
}

// ListDatabases returns all databases in AWS Glue.
// Per phase-7-spec.md §3.2: List databases with pagination.
func (c *Client) ListDatabases(ctx context.Context) ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, fmt.Errorf("glue: client is closed")
	}

	// Note: In production, this would call AWS Glue GetDatabases API
	// with pagination support.
	//
	// For MVP, return error indicating SDK required.
	return nil, fmt.Errorf("glue: AWS SDK not implemented; " +
		"requires github.com/aws/aws-sdk-go-v2 dependency")
}

// ListTables returns all tables in a database.
// Per phase-7-spec.md §3.2: List tables with format detection.
func (c *Client) ListTables(ctx context.Context, database string) ([]catalog.TableInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, fmt.Errorf("glue: client is closed")
	}

	if database == "" {
		return nil, fmt.Errorf("glue: database name is required")
	}

	// Note: In production, this would call AWS Glue GetTables API.
	// For MVP, return error indicating SDK required.
	return nil, fmt.Errorf("glue: AWS SDK not implemented; " +
		"requires github.com/aws/aws-sdk-go-v2 dependency")
}

// GetTable returns detailed metadata for a specific table.
// Per phase-7-spec.md §3: Get table with format detection.
func (c *Client) GetTable(ctx context.Context, database, table string) (*catalog.TableMetadata, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, fmt.Errorf("glue: client is closed")
	}

	if database == "" {
		return nil, fmt.Errorf("glue: database name is required")
	}

	if table == "" {
		return nil, fmt.Errorf("glue: table name is required")
	}

	// Note: In production, this would call AWS Glue GetTable API.
	// For MVP, return error indicating SDK required.
	return nil, fmt.Errorf("glue: AWS SDK not implemented; " +
		"requires github.com/aws/aws-sdk-go-v2 dependency")
}

// Close releases resources.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true
	return nil
}

// ShouldIncludeDatabase checks if a database should be included based on config.
func (c *Client) ShouldIncludeDatabase(database string) bool {
	// Check exclude list first
	for _, excluded := range c.config.ExcludeDatabases {
		if excluded == database {
			return false
		}
	}

	// If include list is empty, include all
	if len(c.config.IncludeDatabases) == 0 {
		return true
	}

	// Check include list
	for _, included := range c.config.IncludeDatabases {
		if included == database {
			return true
		}
	}

	return false
}

// DetectGlueFormat detects table format from Glue table properties.
// Per phase-7-spec.md §3.2: Format detection from Glue metadata.
func DetectGlueFormat(tableType string, parameters map[string]string, location string) catalog.TableFormat {
	// Check table type
	if tableType == "ICEBERG" {
		return catalog.FormatIceberg
	}

	// Check parameters
	if parameters != nil {
		if parameters["table_type"] == "ICEBERG" {
			return catalog.FormatIceberg
		}
		if parameters["spark.sql.sources.provider"] == "delta" {
			return catalog.FormatDelta
		}
		if parameters["spark.sql.sources.provider"] == "hudi" {
			return catalog.FormatHudi
		}
	}

	// Check location for Delta log
	if strings.Contains(location, "_delta_log") {
		return catalog.FormatDelta
	}

	return catalog.FormatUnknown
}

// Verify Client implements catalog.Catalog interface.
var _ catalog.Catalog = (*Client)(nil)
