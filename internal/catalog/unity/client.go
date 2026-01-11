// Package unity provides the Databricks Unity Catalog client.
//
// Per phase-7-spec.md §7: Connect to Databricks Unity Catalog for table discovery.
// Per docs/plan.md: "Adapters are stateless, replaceable, thin."
package unity

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/canonica-labs/canonica/internal/catalog"
)

// Config configures the Unity Catalog client.
// Per phase-7-spec.md §7.1: Unity Catalog configuration.
type Config struct {
	// Host is the Databricks workspace URL.
	// Format: https://adb-1234567890.12.azuredatabricks.net
	Host string

	// Token is the personal access token or service principal token.
	Token string

	// Catalog is the Unity Catalog name (optional, lists all if empty).
	Catalog string

	// RequestTimeout for API calls.
	RequestTimeout time.Duration

	// IncludeSchemas filters which schemas to sync (empty = all).
	IncludeSchemas []string

	// ExcludeSchemas filters which schemas to exclude.
	ExcludeSchemas []string
}

// DefaultConfig returns a default configuration.
func DefaultConfig() Config {
	return Config{
		RequestTimeout: 30 * time.Second,
	}
}

// Validate validates the configuration.
func (c Config) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("unity: host is required")
	}

	// Validate host is a valid URL
	parsed, err := url.Parse(c.Host)
	if err != nil {
		return fmt.Errorf("unity: invalid host URL: %w", err)
	}

	if parsed.Scheme != "https" {
		return fmt.Errorf("unity: host must use https")
	}

	if c.Token == "" {
		return fmt.Errorf("unity: token is required")
	}

	return nil
}

// Client implements the Catalog interface for Databricks Unity Catalog.
// Per phase-7-spec.md §7: Unity Catalog Integration.
type Client struct {
	mu         sync.RWMutex
	config     Config
	httpClient *http.Client
	closed     bool
}

// NewClient creates a new Unity Catalog client.
// Per phase-7-spec.md §7.1: Returns error if configuration is invalid.
func NewClient(config Config) (*Client, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	// Apply defaults
	if config.RequestTimeout <= 0 {
		config.RequestTimeout = 30 * time.Second
	}

	return &Client{
		config: config,
		httpClient: &http.Client{
			Timeout: config.RequestTimeout,
		},
		closed: false,
	}, nil
}

// Name returns the catalog identifier.
func (c *Client) Name() string {
	return "unity"
}

// CheckConnectivity verifies Unity Catalog is reachable.
// Per phase-7-spec.md: Health check validates access token works.
func (c *Client) CheckConnectivity(ctx context.Context) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return fmt.Errorf("unity: client is closed")
	}

	// Try to list catalogs as connectivity check
	_, err := c.request(ctx, "GET", "/api/2.1/unity-catalog/catalogs?max_results=1", nil)
	if err != nil {
		return fmt.Errorf("unity: connectivity check failed: %w", err)
	}

	return nil
}

// ListDatabases returns all schemas (databases) in Unity Catalog.
// For Unity Catalog, we list schemas within the configured catalog.
// Per phase-7-spec.md §7.2: List schemas from Unity Catalog.
func (c *Client) ListDatabases(ctx context.Context) ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, fmt.Errorf("unity: client is closed")
	}

	// If no specific catalog configured, list all catalogs first
	if c.config.Catalog == "" {
		return c.listAllSchemas(ctx)
	}

	// List schemas in the specific catalog
	return c.listSchemasInCatalog(ctx, c.config.Catalog)
}

// listAllSchemas lists schemas from all accessible catalogs.
func (c *Client) listAllSchemas(ctx context.Context) ([]string, error) {
	// First, list catalogs
	catalogs, err := c.listCatalogs(ctx)
	if err != nil {
		return nil, err
	}

	var allSchemas []string
	for _, cat := range catalogs {
		schemas, err := c.listSchemasInCatalog(ctx, cat)
		if err != nil {
			// Log but continue with other catalogs
			continue
		}
		for _, schema := range schemas {
			allSchemas = append(allSchemas, fmt.Sprintf("%s.%s", cat, schema))
		}
	}

	return allSchemas, nil
}

// listCatalogs lists all accessible catalogs.
func (c *Client) listCatalogs(ctx context.Context) ([]string, error) {
	resp, err := c.request(ctx, "GET", "/api/2.1/unity-catalog/catalogs", nil)
	if err != nil {
		return nil, fmt.Errorf("unity: failed to list catalogs: %w", err)
	}

	var result catalogListResponse
	if err := json.Unmarshal(resp, &result); err != nil {
		return nil, fmt.Errorf("unity: failed to parse catalogs response: %w", err)
	}

	var catalogs []string
	for _, cat := range result.Catalogs {
		catalogs = append(catalogs, cat.Name)
	}
	return catalogs, nil
}

// listSchemasInCatalog lists schemas in a specific catalog.
func (c *Client) listSchemasInCatalog(ctx context.Context, catalogName string) ([]string, error) {
	path := fmt.Sprintf("/api/2.1/unity-catalog/schemas?catalog_name=%s", url.QueryEscape(catalogName))
	resp, err := c.request(ctx, "GET", path, nil)
	if err != nil {
		return nil, fmt.Errorf("unity: failed to list schemas: %w", err)
	}

	var result schemaListResponse
	if err := json.Unmarshal(resp, &result); err != nil {
		return nil, fmt.Errorf("unity: failed to parse schemas response: %w", err)
	}

	var schemas []string
	for _, schema := range result.Schemas {
		if c.shouldIncludeSchema(schema.Name) {
			schemas = append(schemas, schema.Name)
		}
	}
	return schemas, nil
}

// ListTables returns all tables in a schema.
// Per phase-7-spec.md §7.2: List tables with format detection.
func (c *Client) ListTables(ctx context.Context, database string) ([]catalog.TableInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, fmt.Errorf("unity: client is closed")
	}

	if database == "" {
		return nil, fmt.Errorf("unity: database (schema) name is required")
	}

	// Parse database as catalog.schema
	catalogName, schemaName := parseDatabaseName(database, c.config.Catalog)

	path := fmt.Sprintf("/api/2.1/unity-catalog/tables?catalog_name=%s&schema_name=%s",
		url.QueryEscape(catalogName),
		url.QueryEscape(schemaName))

	resp, err := c.request(ctx, "GET", path, nil)
	if err != nil {
		return nil, fmt.Errorf("unity: failed to list tables: %w", err)
	}

	var result tableListResponse
	if err := json.Unmarshal(resp, &result); err != nil {
		return nil, fmt.Errorf("unity: failed to parse tables response: %w", err)
	}

	var tables []catalog.TableInfo
	for _, t := range result.Tables {
		tables = append(tables, catalog.TableInfo{
			Database: database,
			Name:     t.Name,
			Format:   detectUnityFormat(t),
		})
	}

	return tables, nil
}

// GetTable returns detailed metadata for a specific table.
// Per phase-7-spec.md §7: Get table with format detection.
func (c *Client) GetTable(ctx context.Context, database, table string) (*catalog.TableMetadata, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, fmt.Errorf("unity: client is closed")
	}

	if database == "" {
		return nil, fmt.Errorf("unity: database (schema) name is required")
	}

	if table == "" {
		return nil, fmt.Errorf("unity: table name is required")
	}

	// Parse database as catalog.schema
	catalogName, schemaName := parseDatabaseName(database, c.config.Catalog)

	fullName := fmt.Sprintf("%s.%s.%s", catalogName, schemaName, table)
	path := fmt.Sprintf("/api/2.1/unity-catalog/tables/%s", url.PathEscape(fullName))

	resp, err := c.request(ctx, "GET", path, nil)
	if err != nil {
		return nil, fmt.Errorf("unity: failed to get table: %w", err)
	}

	var t unityTable
	if err := json.Unmarshal(resp, &t); err != nil {
		return nil, fmt.Errorf("unity: failed to parse table response: %w", err)
	}

	// Convert to TableMetadata
	metadata := &catalog.TableMetadata{
		Database:   database,
		Name:       t.Name,
		Format:     detectUnityFormat(t),
		Location:   t.StorageLocation,
		Properties: t.Properties,
	}

	// Convert columns
	for _, col := range t.Columns {
		metadata.Columns = append(metadata.Columns, catalog.ColumnMetadata{
			Name:     col.Name,
			Type:     col.TypeText,
			Nullable: col.Nullable,
			Comment:  col.Comment,
		})
	}

	return metadata, nil
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

// request makes an HTTP request to the Unity Catalog API.
func (c *Client) request(ctx context.Context, method, path string, body io.Reader) ([]byte, error) {
	fullURL := strings.TrimSuffix(c.config.Host, "/") + path

	req, err := http.NewRequestWithContext(ctx, method, fullURL, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.config.Token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("API error (status %d): %s", resp.StatusCode, string(respBody))
	}

	return respBody, nil
}

// shouldIncludeSchema checks if a schema should be included based on config.
func (c *Client) shouldIncludeSchema(schema string) bool {
	// Check exclude list first
	for _, excluded := range c.config.ExcludeSchemas {
		if excluded == schema {
			return false
		}
	}

	// If include list is empty, include all
	if len(c.config.IncludeSchemas) == 0 {
		return true
	}

	// Check include list
	for _, included := range c.config.IncludeSchemas {
		if included == schema {
			return true
		}
	}

	return false
}

// parseDatabaseName parses a database name, handling catalog.schema format.
func parseDatabaseName(database, defaultCatalog string) (catalogName, schemaName string) {
	parts := strings.SplitN(database, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}

	if defaultCatalog != "" {
		return defaultCatalog, database
	}

	return "main", database // Unity Catalog default catalog
}

// detectUnityFormat detects table format from Unity Catalog table info.
func detectUnityFormat(t unityTable) catalog.TableFormat {
	// Check data source format
	switch strings.ToLower(t.DataSourceFormat) {
	case "delta":
		return catalog.FormatDelta
	case "iceberg":
		return catalog.FormatIceberg
	case "hudi":
		return catalog.FormatHudi
	case "parquet":
		return catalog.FormatParquet
	case "csv":
		return catalog.FormatCSV
	case "orc":
		return catalog.FormatORC
	}

	// Check table type
	if t.TableType == "MANAGED" || t.TableType == "EXTERNAL" {
		// Unity Catalog managed tables are typically Delta
		return catalog.FormatDelta
	}

	return catalog.FormatUnknown
}

// API response types

type catalogListResponse struct {
	Catalogs []unityCatalog `json:"catalogs"`
}

type unityCatalog struct {
	Name string `json:"name"`
}

type schemaListResponse struct {
	Schemas []unitySchema `json:"schemas"`
}

type unitySchema struct {
	Name string `json:"name"`
}

type tableListResponse struct {
	Tables []unityTable `json:"tables"`
}

type unityTable struct {
	Name             string            `json:"name"`
	CatalogName      string            `json:"catalog_name"`
	SchemaName       string            `json:"schema_name"`
	TableType        string            `json:"table_type"`
	DataSourceFormat string            `json:"data_source_format"`
	StorageLocation  string            `json:"storage_location"`
	Columns          []unityColumn     `json:"columns"`
	Properties       map[string]string `json:"properties"`
}

type unityColumn struct {
	Name     string `json:"name"`
	TypeText string `json:"type_text"`
	Nullable bool   `json:"nullable"`
	Comment  string `json:"comment"`
}

// Verify Client implements catalog.Catalog interface.
var _ catalog.Catalog = (*Client)(nil)
