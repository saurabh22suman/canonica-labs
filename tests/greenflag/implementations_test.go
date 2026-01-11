// Package greenflag contains tests that verify features work correctly.
// Per docs/test.md: "Green-Flag tests validate happy paths."
//
// This file tests T003: Spark Adapter Wiring, T030: Audit Persistence,
// T033: Time-Travel Normalization, T058/T059: Warehouse Drivers.
package greenflag

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/canonica-labs/canonica/internal/adapters"
	"github.com/canonica-labs/canonica/internal/adapters/bigquery"
	"github.com/canonica-labs/canonica/internal/adapters/snowflake"
	"github.com/canonica-labs/canonica/internal/adapters/spark"
	"github.com/canonica-labs/canonica/internal/catalog"
	"github.com/canonica-labs/canonica/internal/observability"
	canonicsql "github.com/canonica-labs/canonica/internal/sql"

	_ "modernc.org/sqlite"
)

// ============== T003: Spark Adapter Wiring ==============

// TestSparkAdapter_RegistryIntegration verifies Spark works with adapter registry.
func TestSparkAdapter_RegistryIntegration(t *testing.T) {
	registry := adapters.NewAdapterRegistry()

	// Register multiple adapters
	registry.Register(spark.NewAdapter(spark.AdapterConfig{Host: "spark.local", Port: 10000}))

	// Verify available adapters
	available := registry.Available()
	found := false
	for _, name := range available {
		if name == "spark" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Spark adapter not found in available adapters")
	}
}

// TestSparkAdapter_CapabilitiesComplete verifies all expected capabilities.
func TestSparkAdapter_CapabilitiesComplete(t *testing.T) {
	adapter := spark.NewAdapter(spark.AdapterConfig{Host: "localhost", Port: 10000})
	caps := adapter.Capabilities()

	// Spark should support these capabilities
	expectedCaps := map[string]bool{
		"READ":        false,
		"TIME_TRAVEL": false,
	}

	for _, cap := range caps {
		name := cap.String()
		if _, expected := expectedCaps[name]; expected {
			expectedCaps[name] = true
		}
	}

	for name, found := range expectedCaps {
		if !found {
			t.Errorf("Expected capability %s not found", name)
		}
	}
}

// ============== T030: Audit Log Persistence ==============

// TestPersistentLogger_FullWorkflow verifies complete audit logging workflow.
func TestPersistentLogger_FullWorkflow(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open SQLite: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(`CREATE TABLE audit_logs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		query_id TEXT NOT NULL,
		user_id TEXT NOT NULL,
		role TEXT,
		tables_json TEXT DEFAULT '[]',
		auth_decision TEXT,
		planner_decision TEXT,
		engine TEXT,
		execution_time_ms INTEGER DEFAULT 0,
		outcome TEXT,
		error_message TEXT,
		invariant_violated TEXT,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	logger, err := observability.NewPersistentLogger(db)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}

	ctx := context.Background()

	// Log successful queries
	for i := 0; i < 5; i++ {
		_ = logger.LogQuery(ctx, observability.QueryLogEntry{
			QueryID:       "q-success-" + string(rune('0'+i)),
			User:          "user1",
			Role:          "analyst",
			Tables:        []string{"sales.orders"},
			Engine:        "duckdb",
			ExecutionTime: 50 * time.Millisecond,
			Outcome:       "success",
		})
	}

	// Log failed queries
	for i := 0; i < 3; i++ {
		_ = logger.LogQuery(ctx, observability.QueryLogEntry{
			QueryID:       "q-error-" + string(rune('0'+i)),
			User:          "user2",
			Role:          "analyst",
			ExecutionTime: 10 * time.Millisecond,
			Outcome:       "error",
			Error:         "unauthorized access",
		})
	}

	// Verify counts
	var successCount, errorCount int
	db.QueryRow("SELECT COUNT(*) FROM audit_logs WHERE outcome = 'success'").Scan(&successCount)
	db.QueryRow("SELECT COUNT(*) FROM audit_logs WHERE outcome = 'error'").Scan(&errorCount)

	if successCount != 5 {
		t.Errorf("Expected 5 success entries, got %d", successCount)
	}
	if errorCount != 3 {
		t.Errorf("Expected 3 error entries, got %d", errorCount)
	}
}

// ============== T033: Time-Travel Normalization ==============

// TestTimeTravelRewriter_AllFormats verifies rewriting for all supported formats.
func TestTimeTravelRewriter_AllFormats(t *testing.T) {
	testCases := []struct {
		format   catalog.TableFormat
		engine   string
		input    string
		contains string
	}{
		{catalog.FormatIceberg, "trino", "SELECT * FROM t FOR SYSTEM_TIME AS OF '2026-01-01T00:00:00Z'", "FOR TIMESTAMP AS OF TIMESTAMP"},
		{catalog.FormatIceberg, "spark", "SELECT * FROM t FOR SYSTEM_TIME AS OF '2026-01-01T00:00:00Z'", "TIMESTAMP AS OF"},
		{catalog.FormatIceberg, "duckdb", "SELECT * FROM t FOR SYSTEM_TIME AS OF '2026-01-01T00:00:00Z'", "AT TIMESTAMP"},
		{catalog.FormatDelta, "spark", "SELECT * FROM t FOR SYSTEM_TIME AS OF '2026-01-01T00:00:00Z'", "TIMESTAMP AS OF"},
		{catalog.FormatDelta, "duckdb", "SELECT * FROM t FOR SYSTEM_TIME AS OF '2026-01-01T00:00:00Z'", "AT TIMESTAMP"},
	}

	for _, tc := range testCases {
		t.Run(string(tc.format)+"-"+tc.engine, func(t *testing.T) {
			rewriter := canonicsql.NewTimeTravelRewriter(tc.format, tc.engine)
			result, err := rewriter.Rewrite(tc.input)
			if err != nil {
				t.Fatalf("Rewrite failed: %v", err)
			}
			if !strings.Contains(result, tc.contains) {
				t.Errorf("Expected result to contain %q, got: %s", tc.contains, result)
			}
		})
	}
}

// TestWarehouseRewriter_AllWarehouses verifies warehouse-specific rewriting.
func TestWarehouseRewriter_AllWarehouses(t *testing.T) {
	testCases := []struct {
		warehouse string
		input     string
		contains  string
		expectErr bool
	}{
		{"snowflake", "SELECT * FROM t FOR SYSTEM_TIME AS OF '2026-01-01T00:00:00Z'", "AT(TIMESTAMP =>", false},
		{"bigquery", "SELECT * FROM t FOR SYSTEM_TIME AS OF '2026-01-01T00:00:00Z'", "FOR SYSTEM_TIME AS OF TIMESTAMP", false},
		{"redshift", "SELECT * FROM t FOR SYSTEM_TIME AS OF '2026-01-01T00:00:00Z'", "", true}, // Redshift doesn't support
	}

	for _, tc := range testCases {
		t.Run(tc.warehouse, func(t *testing.T) {
			rewriter := canonicsql.NewWarehouseRewriter(tc.warehouse)
			result, err := rewriter.Rewrite(tc.input)

			if tc.expectErr {
				if err == nil {
					t.Error("Expected error, got nil")
				}
			} else {
				if err != nil {
					t.Fatalf("Rewrite failed: %v", err)
				}
				if !strings.Contains(result, tc.contains) {
					t.Errorf("Expected result to contain %q, got: %s", tc.contains, result)
				}
			}
		})
	}
}

// ============== T058/T059: Warehouse Drivers ==============

// TestSnowflakeAdapter_Configuration verifies Snowflake config validation.
func TestSnowflakeAdapter_Configuration(t *testing.T) {
	// Valid config with all fields
	config := snowflake.Config{
		Account:        "test.us-east-1",
		User:           "testuser",
		Password:       "testpass",
		Database:       "testdb",
		Schema:         "public",
		Warehouse:      "compute_wh",
		Role:           "analyst",
		ConnectTimeout: 30 * time.Second,
		QueryTimeout:   5 * time.Minute,
	}

	// Should validate without error
	if err := config.Validate(); err != nil {
		t.Errorf("Valid config failed validation: %v", err)
	}

	// Test default config
	defaultCfg := snowflake.DefaultConfig()
	if defaultCfg.ConnectTimeout == 0 {
		t.Error("Default config should have non-zero connect timeout")
	}
}

// TestBigQueryAdapter_Configuration verifies BigQuery config validation.
func TestBigQueryAdapter_Configuration(t *testing.T) {
	// Valid config
	config := bigquery.Config{
		ProjectID:      "my-gcp-project",
		Location:       "US",
		DefaultDataset: "analytics",
		QueryTimeout:   5 * time.Minute,
	}

	if err := config.Validate(); err != nil {
		t.Errorf("Valid config failed validation: %v", err)
	}

	// Test default config
	defaultCfg := bigquery.DefaultConfig()
	if defaultCfg.Location == "" {
		t.Error("Default config should have location set")
	}
}

// TestWarehouseAdapters_Capabilities verifies warehouse capabilities.
func TestWarehouseAdapters_Capabilities(t *testing.T) {
	// Snowflake capabilities
	snowflakeAdapter := snowflake.NewAdapterWithoutConnect(snowflake.Config{
		Account:   "test",
		User:      "test",
		Password:  "test",
		Warehouse: "test",
	})
	sfCaps := snowflakeAdapter.Capabilities()
	if len(sfCaps) == 0 {
		t.Error("Snowflake should report capabilities")
	}

	// BigQuery capabilities
	bqAdapter := bigquery.NewAdapterWithoutConnect(bigquery.Config{
		ProjectID: "test",
	})
	bqCaps := bqAdapter.Capabilities()
	if len(bqCaps) == 0 {
		t.Error("BigQuery should report capabilities")
	}
}
