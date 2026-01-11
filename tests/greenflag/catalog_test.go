// Package greenflag contains green-flag tests that verify successful behavior.
// Per test.md: Green-Flag tests validate happy-path functionality.
package greenflag

import (
	"context"
	"testing"
	"time"

	"github.com/canonica-labs/canonica/internal/catalog"
	"github.com/canonica-labs/canonica/internal/catalog/hive"
	"github.com/canonica-labs/canonica/internal/catalog/unity"
)

// TestFormatDetectionIceberg verifies Iceberg format detection from properties.
// Per phase-7-spec.md §2.5: Green-Flag tests for format detection.
func TestFormatDetectionIceberg(t *testing.T) {
	testCases := []struct {
		name       string
		properties map[string]string
		serdeLib   string
		expected   catalog.TableFormat
	}{
		{
			name: "iceberg table_type property",
			properties: map[string]string{
				"table_type": "ICEBERG",
			},
			expected: catalog.FormatIceberg,
		},
		{
			name: "iceberg metadata_location property",
			properties: map[string]string{
				"metadata_location": "s3://bucket/iceberg-table/metadata/00001.json",
			},
			expected: catalog.FormatIceberg,
		},
		{
			name: "iceberg serde",
			properties: map[string]string{},
			serdeLib:   "org.apache.iceberg.mr.hive.HiveIcebergSerDe",
			expected:   catalog.FormatIceberg,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			format := catalog.DetectFormatFromProperties(tc.properties, tc.serdeLib)
			if format != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, format)
			}
		})
	}
}

// TestFormatDetectionDelta verifies Delta format detection from properties.
// Per phase-7-spec.md §2.5: Green-Flag tests for format detection.
func TestFormatDetectionDelta(t *testing.T) {
	testCases := []struct {
		name       string
		properties map[string]string
		serdeLib   string
		expected   catalog.TableFormat
	}{
		{
			name: "delta table_type property",
			properties: map[string]string{
				"table_type": "DELTA",
			},
			expected: catalog.FormatDelta,
		},
		{
			name: "delta spark.sql.sources.provider",
			properties: map[string]string{
				"spark.sql.sources.provider": "delta",
			},
			expected: catalog.FormatDelta,
		},
		{
			name: "delta serde",
			properties: map[string]string{},
			serdeLib:   "io.delta.hive.DeltaStorageHandler",
			expected:   catalog.FormatDelta,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			format := catalog.DetectFormatFromProperties(tc.properties, tc.serdeLib)
			if format != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, format)
			}
		})
	}
}

// TestFormatDetectionHudi verifies Hudi format detection from properties.
// Per phase-7-spec.md §2.5: Green-Flag tests for format detection.
func TestFormatDetectionHudi(t *testing.T) {
	testCases := []struct {
		name       string
		properties map[string]string
		serdeLib   string
		expected   catalog.TableFormat
	}{
		{
			name: "hudi table_type property",
			properties: map[string]string{
				"table_type": "HUDI",
			},
			expected: catalog.FormatHudi,
		},
		{
			name: "hudi spark.sql.sources.provider",
			properties: map[string]string{
				"spark.sql.sources.provider": "hudi",
			},
			expected: catalog.FormatHudi,
		},
		{
			name: "hudi serde",
			properties: map[string]string{},
			serdeLib:   "org.apache.hudi.hadoop.HoodieParquetInputFormat",
			expected:   catalog.FormatHudi,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			format := catalog.DetectFormatFromProperties(tc.properties, tc.serdeLib)
			if format != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, format)
			}
		})
	}
}

// TestFormatDetectionFromLocation verifies format detection from location hints.
// Per phase-7-spec.md §2.5: Green-Flag tests for format detection.
func TestFormatDetectionFromLocation(t *testing.T) {
	testCases := []struct {
		name     string
		location string
		expected catalog.TableFormat
	}{
		{
			name:     "parquet file extension",
			location: "s3://bucket/data/file.parquet",
			expected: catalog.FormatParquet,
		},
		{
			name:     "orc file extension",
			location: "s3://bucket/data/file.orc",
			expected: catalog.FormatORC,
		},
		{
			name:     "csv file extension",
			location: "s3://bucket/data/file.csv",
			expected: catalog.FormatCSV,
		},
		{
			name:     "delta log directory hint",
			location: "s3://bucket/delta-table/_delta_log/",
			expected: catalog.FormatDelta,
		},
		{
			name:     "iceberg metadata hint",
			location: "s3://bucket/iceberg-table/metadata/",
			expected: catalog.FormatIceberg,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			format := catalog.DetectFormatFromLocationHint(tc.location)
			if format != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, format)
			}
		})
	}
}

// TestSelectEngine verifies engine selection for different formats.
// Per phase-7-spec.md §2.5: Green-Flag tests for engine routing.
func TestSelectEngine(t *testing.T) {
	testCases := []struct {
		format         catalog.TableFormat
		expectedEngine string
	}{
		{catalog.FormatIceberg, "trino"},
		{catalog.FormatDelta, "trino"},
		{catalog.FormatHudi, "trino"},
		{catalog.FormatParquet, "duckdb"},
		{catalog.FormatORC, "trino"},     // ORC routes to Trino (DuckDB has limited ORC support)
		{catalog.FormatCSV, "duckdb"},
		{catalog.FormatUnknown, "duckdb"}, // Default to DuckDB
	}

	for _, tc := range testCases {
		t.Run(string(tc.format), func(t *testing.T) {
			engine := catalog.SelectEngine(tc.format)
			if engine != tc.expectedEngine {
				t.Errorf("expected %s for format %s, got %s", tc.expectedEngine, tc.format, engine)
			}
		})
	}
}

// TestCatalogRegistry verifies the CatalogRegistry functionality.
// Per phase-7-spec.md §2.5: Green-Flag tests for registry operations.
func TestCatalogRegistry(t *testing.T) {
	registry := catalog.NewCatalogRegistry()

	// Create mock catalogs
	mock1 := &mockCatalog{name: "catalog-1"}
	mock2 := &mockCatalog{name: "catalog-2"}
	mock3 := &mockCatalog{name: "catalog-3"}

	// Register catalogs
	registry.Register(mock1)
	registry.Register(mock2)
	registry.Register(mock3)

	// Verify List returns all catalogs
	list := registry.List()
	if len(list) != 3 {
		t.Errorf("expected 3 catalogs, got %d", len(list))
	}

	// Verify Get retrieves correct catalogs
	cat1, ok := registry.Get("catalog-1")
	if !ok {
		t.Error("expected catalog-1 to be found")
	}
	if cat1.Name() != "catalog-1" {
		t.Errorf("expected name catalog-1, got %s", cat1.Name())
	}

	cat2, ok := registry.Get("catalog-2")
	if !ok {
		t.Error("expected catalog-2 to be found")
	}
	if cat2.Name() != "catalog-2" {
		t.Errorf("expected name catalog-2, got %s", cat2.Name())
	}

	// Verify Get returns false for non-existent
	_, ok = registry.Get("catalog-99")
	if ok {
		t.Error("expected catalog-99 to not be found")
	}

	// Verify All returns all catalogs
	all := registry.All()
	if len(all) != 3 {
		t.Errorf("expected 3 catalogs from All(), got %d", len(all))
	}
}

// TestTableMetadataFullName verifies full name generation.
// Per phase-7-spec.md §2.5: Green-Flag tests for metadata helpers.
func TestTableMetadataFullName(t *testing.T) {
	testCases := []struct {
		meta     catalog.TableMetadata
		expected string
	}{
		{
			meta:     catalog.TableMetadata{Database: "db", Name: "table"},
			expected: "db.table",
		},
		{
			meta:     catalog.TableMetadata{Database: "analytics", Name: "events"},
			expected: "analytics.events",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			result := tc.meta.FullName()
			if result != tc.expected {
				t.Errorf("expected %s, got %s", tc.expected, result)
			}
		})
	}
}

// TestHiveClientConfig verifies Hive client configuration validation.
// Per phase-7-spec.md §2.5: Green-Flag tests for valid configurations.
func TestHiveClientConfig(t *testing.T) {
	// Valid configuration should not error
	cfg := hive.Config{
		ThriftURI:      "thrift://localhost:9083",
		ConnectTimeout: 10 * time.Second,
		RequestTimeout: 30 * time.Second,
	}

	err := cfg.Validate()
	if err != nil {
		t.Errorf("expected valid config to pass validation: %v", err)
	}

	// DefaultConfig should be usable with ThriftURI
	defaultCfg := hive.DefaultConfig()
	defaultCfg.ThriftURI = "thrift://localhost:9083"

	err = defaultCfg.Validate()
	if err != nil {
		t.Errorf("expected default config with URI to pass validation: %v", err)
	}
}

// TestUnityClientConfig verifies Unity client configuration validation.
// Per phase-7-spec.md §2.5: Green-Flag tests for valid configurations.
func TestUnityClientConfig(t *testing.T) {
	// Valid configuration should create client
	cfg := unity.Config{
		Host:           "https://workspace.azuredatabricks.net",
		Token:          "dapi-xxxx-yyyy",
		RequestTimeout: 30 * time.Second,
	}

	err := cfg.Validate()
	if err != nil {
		t.Errorf("expected valid config to pass validation: %v", err)
	}

	// DefaultConfig should be usable with Host and Token
	defaultCfg := unity.DefaultConfig()
	defaultCfg.Host = "https://workspace.azuredatabricks.net"
	defaultCfg.Token = "dapi-xxxx-yyyy"

	err = defaultCfg.Validate()
	if err != nil {
		t.Errorf("expected default config with host/token to pass validation: %v", err)
	}
}

// TestCatalogInterface verifies mock catalog implements interface.
// Per phase-7-spec.md §2.5: Interface compliance verification.
func TestCatalogInterface(t *testing.T) {
	mock := &mockCatalog{
		name:      "test-catalog",
		databases: []string{"db1", "db2"},
	}

	// Verify interface implementation
	var cat catalog.Catalog = mock
	_ = cat

	// Test all methods
	ctx := context.Background()

	name := mock.Name()
	if name != "test-catalog" {
		t.Errorf("expected name test-catalog, got %s", name)
	}

	dbs, err := mock.ListDatabases(ctx)
	if err != nil {
		t.Errorf("ListDatabases failed: %v", err)
	}
	if len(dbs) != 2 {
		t.Errorf("expected 2 databases, got %d", len(dbs))
	}

	tables, err := mock.ListTables(ctx, "db1")
	if err != nil {
		t.Errorf("ListTables failed: %v", err)
	}
	_ = tables // Mock returns empty

	err = mock.CheckConnectivity(ctx)
	if err != nil {
		t.Errorf("CheckConnectivity failed: %v", err)
	}

	err = mock.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

// TestTableFormat verifies TableFormat string representation.
// Per phase-7-spec.md §2.5: Type verification tests.
func TestTableFormat(t *testing.T) {
	testCases := []struct {
		format   catalog.TableFormat
		expected string
	}{
		{catalog.FormatIceberg, "iceberg"},
		{catalog.FormatDelta, "delta"},
		{catalog.FormatHudi, "hudi"},
		{catalog.FormatParquet, "parquet"},
		{catalog.FormatORC, "orc"},
		{catalog.FormatCSV, "csv"},
		{catalog.FormatUnknown, "unknown"},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			result := string(tc.format)
			if result != tc.expected {
				t.Errorf("expected %s, got %s", tc.expected, result)
			}
		})
	}
}

// mockCatalog is a simple mock implementation for testing.
type mockCatalog struct {
	name      string
	databases []string
}

func (m *mockCatalog) Name() string {
	return m.name
}

func (m *mockCatalog) ListDatabases(ctx context.Context) ([]string, error) {
	return m.databases, nil
}

func (m *mockCatalog) ListTables(ctx context.Context, database string) ([]catalog.TableInfo, error) {
	return nil, nil
}

func (m *mockCatalog) GetTable(ctx context.Context, database, table string) (*catalog.TableMetadata, error) {
	return nil, nil
}

func (m *mockCatalog) CheckConnectivity(ctx context.Context) error {
	return nil
}

func (m *mockCatalog) Close() error {
	return nil
}

// Ensure mockCatalog implements Catalog interface
var _ catalog.Catalog = (*mockCatalog)(nil)
