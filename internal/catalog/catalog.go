// Package catalog provides the unified interface for external metadata catalogs.
//
// Per phase-7-spec.md: Canonic automatically discovers and syncs tables from
// Hive Metastore, AWS Glue, or Unity Catalog.
//
// Per docs/plan.md: "Control Plane Only" - this package reads metadata,
// it does NOT read or write data files.
package catalog

import (
	"context"
	"time"
)

// Catalog represents an external metadata catalog.
// Per phase-7-spec.md §1.1: Unified interface for interacting with different metadata catalogs.
type Catalog interface {
	// Name returns the catalog identifier (e.g., "hive", "glue", "unity").
	Name() string

	// ListDatabases returns all databases/schemas in the catalog.
	ListDatabases(ctx context.Context) ([]string, error)

	// ListTables returns all tables in a database.
	ListTables(ctx context.Context, database string) ([]TableInfo, error)

	// GetTable returns detailed metadata for a specific table.
	GetTable(ctx context.Context, database, table string) (*TableMetadata, error)

	// CheckConnectivity verifies the catalog is reachable.
	CheckConnectivity(ctx context.Context) error

	// Close releases resources.
	Close() error
}

// TableInfo is a lightweight table reference.
// Per phase-7-spec.md: Used for listing tables without full metadata.
type TableInfo struct {
	Database string      `json:"database"`
	Name     string      `json:"name"`
	Format   TableFormat `json:"format"`
}

// FullName returns the schema-qualified table name.
func (t TableInfo) FullName() string {
	return t.Database + "." + t.Name
}

// TableMetadata is detailed table information.
// Per phase-7-spec.md: Contains all information needed to register a table in Canonic.
type TableMetadata struct {
	Database   string            `json:"database"`
	Name       string            `json:"name"`
	Format     TableFormat       `json:"format"`
	Location   string            `json:"location"` // s3://bucket/path or hdfs://path
	Columns    []ColumnMetadata  `json:"columns"`
	Partitions []string          `json:"partitions"`
	Properties map[string]string `json:"properties"`
	CreatedAt  time.Time         `json:"created_at"`
	UpdatedAt  time.Time         `json:"updated_at"`
}

// FullName returns the schema-qualified table name.
func (t TableMetadata) FullName() string {
	return t.Database + "." + t.Name
}

// ColumnMetadata describes a table column.
type ColumnMetadata struct {
	Name     string `json:"name"`
	Type     string `json:"type"` // Trino/Spark type string
	Nullable bool   `json:"nullable"`
	Comment  string `json:"comment,omitempty"`
}

// TableFormat identifies the table format.
// Per phase-7-spec.md: Canonic automatically detects Iceberg, Delta, Hudi, etc.
type TableFormat string

const (
	FormatIceberg TableFormat = "iceberg"
	FormatDelta   TableFormat = "delta"
	FormatHudi    TableFormat = "hudi"
	FormatParquet TableFormat = "parquet"
	FormatORC     TableFormat = "orc"
	FormatCSV     TableFormat = "csv"
	FormatUnknown TableFormat = "unknown"
)

// String returns the format name.
func (f TableFormat) String() string {
	return string(f)
}

// IsLakehouse returns true if the format supports time-travel and ACID.
func (f TableFormat) IsLakehouse() bool {
	switch f {
	case FormatIceberg, FormatDelta, FormatHudi:
		return true
	default:
		return false
	}
}

// CatalogRegistry holds registered catalogs.
// Per phase-7-spec.md: Multiple catalogs can be configured.
type CatalogRegistry struct {
	catalogs map[string]Catalog
}

// NewCatalogRegistry creates a new catalog registry.
func NewCatalogRegistry() *CatalogRegistry {
	return &CatalogRegistry{
		catalogs: make(map[string]Catalog),
	}
}

// Register adds a catalog to the registry.
func (r *CatalogRegistry) Register(catalog Catalog) {
	r.catalogs[catalog.Name()] = catalog
}

// Get returns a catalog by name.
func (r *CatalogRegistry) Get(name string) (Catalog, bool) {
	cat, ok := r.catalogs[name]
	return cat, ok
}

// List returns all registered catalog names.
func (r *CatalogRegistry) List() []string {
	names := make([]string, 0, len(r.catalogs))
	for name := range r.catalogs {
		names = append(names, name)
	}
	return names
}

// All returns all registered catalogs.
func (r *CatalogRegistry) All() []Catalog {
	cats := make([]Catalog, 0, len(r.catalogs))
	for _, cat := range r.catalogs {
		cats = append(cats, cat)
	}
	return cats
}

// Close closes all registered catalogs.
func (r *CatalogRegistry) Close() error {
	var lastErr error
	for _, cat := range r.catalogs {
		if err := cat.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}
