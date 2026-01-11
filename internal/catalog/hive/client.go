// Package hive provides the Hive Metastore catalog client.
//
// Per phase-7-spec.md §2: Connect to Apache Hive Metastore to discover tables.
// Per docs/plan.md: "Adapters are stateless, replaceable, thin."
package hive

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/canonica-labs/canonica/internal/catalog"
)

// Config configures the Hive Metastore client.
// Per phase-7-spec.md §2.1: Thrift connection configuration.
type Config struct {
	// ThriftURI is the Hive Metastore Thrift endpoint.
	// Format: thrift://host:port
	ThriftURI string

	// ConnectTimeout for initial connection.
	ConnectTimeout time.Duration

	// RequestTimeout for each request.
	RequestTimeout time.Duration

	// IncludeDatabases filters which databases to sync (empty = all).
	IncludeDatabases []string

	// ExcludeDatabases filters which databases to exclude.
	ExcludeDatabases []string
}

// DefaultConfig returns a default configuration.
func DefaultConfig() Config {
	return Config{
		ConnectTimeout: 10 * time.Second,
		RequestTimeout: 30 * time.Second,
	}
}

// Validate validates the configuration.
func (c Config) Validate() error {
	if c.ThriftURI == "" {
		return fmt.Errorf("hive: thrift_uri is required")
	}

	_, _, err := parseThriftURI(c.ThriftURI)
	if err != nil {
		return fmt.Errorf("hive: invalid thrift_uri: %w", err)
	}

	return nil
}

// parseThriftURI parses thrift://host:port into host and port.
func parseThriftURI(uri string) (string, int, error) {
	if !strings.HasPrefix(uri, "thrift://") {
		return "", 0, fmt.Errorf("URI must start with thrift://")
	}

	// Parse as URL
	parsed, err := url.Parse(uri)
	if err != nil {
		return "", 0, err
	}

	host := parsed.Hostname()
	if host == "" {
		return "", 0, fmt.Errorf("missing host in URI")
	}

	portStr := parsed.Port()
	if portStr == "" {
		return "", 0, fmt.Errorf("missing port in URI")
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, fmt.Errorf("invalid port: %w", err)
	}

	return host, port, nil
}

// Client implements the Catalog interface for Hive Metastore.
// Per phase-7-spec.md §2: Hive Metastore Integration.
type Client struct {
	mu     sync.RWMutex
	config Config
	host   string
	port   int
	closed bool

	// Note: In production, this would use Thrift client.
	// For MVP, we simulate the interface with error handling.
}

// NewClient creates a new Hive Metastore client.
// Per phase-7-spec.md §2.1: Returns error if configuration is invalid.
func NewClient(config Config) (*Client, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	host, port, err := parseThriftURI(config.ThriftURI)
	if err != nil {
		return nil, fmt.Errorf("hive: invalid URI %q: %w", config.ThriftURI, err)
	}

	// Apply defaults
	if config.ConnectTimeout <= 0 {
		config.ConnectTimeout = 10 * time.Second
	}
	if config.RequestTimeout <= 0 {
		config.RequestTimeout = 30 * time.Second
	}

	return &Client{
		config: config,
		host:   host,
		port:   port,
		closed: false,
	}, nil
}

// Name returns the catalog identifier.
func (c *Client) Name() string {
	return "hive"
}

// CheckConnectivity verifies the Hive Metastore is reachable.
// Per phase-7-spec.md: Health check validates server is accessible.
func (c *Client) CheckConnectivity(ctx context.Context) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return fmt.Errorf("hive: client is closed")
	}

	// Create timeout context
	dialCtx, cancel := context.WithTimeout(ctx, c.config.ConnectTimeout)
	defer cancel()

	// Attempt TCP connection to verify server is reachable
	address := fmt.Sprintf("%s:%d", c.host, c.port)
	dialer := &net.Dialer{}

	conn, err := dialer.DialContext(dialCtx, "tcp", address)
	if err != nil {
		return fmt.Errorf("hive: connection failed to %s: %w; "+
			"verify the Hive Metastore is running and accessible", c.config.ThriftURI, err)
	}
	conn.Close()

	return nil
}

// ListDatabases returns all databases/schemas in the catalog.
// Per phase-7-spec.md §2: List databases with filtering support.
func (c *Client) ListDatabases(ctx context.Context) ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, fmt.Errorf("hive: client is closed")
	}

	// Check connectivity first
	if err := c.checkConnectivityUnlocked(ctx); err != nil {
		return nil, err
	}

	// Note: In production, this would call Thrift API:
	// databases, err := c.thriftClient.GetAllDatabases(ctx)
	//
	// For MVP without Thrift dependency, we return an error indicating
	// Thrift library is required.
	return nil, fmt.Errorf("hive: Thrift client not implemented; " +
		"requires github.com/apache/thrift dependency; " +
		"see phase-7-spec.md Appendix B for required dependencies")
}

// ListTables returns all tables in a database.
// Per phase-7-spec.md §2: List tables with format detection.
func (c *Client) ListTables(ctx context.Context, database string) ([]catalog.TableInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, fmt.Errorf("hive: client is closed")
	}

	if database == "" {
		return nil, fmt.Errorf("hive: database name is required")
	}

	// Check connectivity first
	if err := c.checkConnectivityUnlocked(ctx); err != nil {
		return nil, err
	}

	// Note: In production, this would call Thrift API.
	// For MVP, return error indicating Thrift required.
	return nil, fmt.Errorf("hive: Thrift client not implemented; " +
		"requires github.com/apache/thrift dependency")
}

// GetTable returns detailed metadata for a specific table.
// Per phase-7-spec.md §2: Get table with format detection.
func (c *Client) GetTable(ctx context.Context, database, table string) (*catalog.TableMetadata, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, fmt.Errorf("hive: client is closed")
	}

	if database == "" {
		return nil, fmt.Errorf("hive: database name is required")
	}

	if table == "" {
		return nil, fmt.Errorf("hive: table name is required")
	}

	// Check connectivity first
	if err := c.checkConnectivityUnlocked(ctx); err != nil {
		return nil, err
	}

	// Note: In production, this would call Thrift API.
	// For MVP, return error indicating Thrift required.
	return nil, fmt.Errorf("hive: Thrift client not implemented; " +
		"requires github.com/apache/thrift dependency")
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

// checkConnectivityUnlocked checks connectivity without acquiring lock.
// Caller must hold at least read lock.
func (c *Client) checkConnectivityUnlocked(ctx context.Context) error {
	dialCtx, cancel := context.WithTimeout(ctx, c.config.ConnectTimeout)
	defer cancel()

	address := fmt.Sprintf("%s:%d", c.host, c.port)
	dialer := &net.Dialer{}

	conn, err := dialer.DialContext(dialCtx, "tcp", address)
	if err != nil {
		return fmt.Errorf("hive: connection failed to %s: %w; "+
			"verify the Hive Metastore is running and accessible", c.config.ThriftURI, err)
	}
	conn.Close()

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

// Verify Client implements catalog.Catalog interface.
var _ catalog.Catalog = (*Client)(nil)
