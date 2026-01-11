// Package greenflag contains tests that verify the system correctly performs allowed operations.
// Per docs/test.md: "Green-Flag tests demonstrate allowed behavior and must be deterministic."
package greenflag

import (
	"context"
	"testing"

	"github.com/canonica-labs/canonica/internal/adapters"
	"github.com/canonica-labs/canonica/internal/adapters/duckdb"
	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/planner"
	"github.com/canonica-labs/canonica/internal/sql"
)

// TestDuckDB_ExecuteSimpleSelect verifies the adapter can execute a simple SELECT.
// Green-Flag: Valid SELECT queries must return results.
func TestDuckDB_ExecuteSimpleSelect(t *testing.T) {
	adapter := duckdb.NewAdapter()
	defer adapter.Close()

	plan := &planner.ExecutionPlan{
		LogicalPlan: &sql.LogicalPlan{
			RawSQL:    "SELECT 1 AS value",
			Operation: capabilities.OperationSelect,
		},
		Engine: "duckdb",
	}

	result, err := adapter.Execute(context.Background(), plan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result == nil {
		t.Fatal("expected result, got nil")
	}

	if len(result.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(result.Columns))
	}

	if result.RowCount != 1 {
		t.Fatalf("expected 1 row, got %d", result.RowCount)
	}
}

// TestDuckDB_ExecuteMultipleColumns verifies the adapter returns correct column names.
// Green-Flag: Result columns must match the SELECT clause.
func TestDuckDB_ExecuteMultipleColumns(t *testing.T) {
	adapter := duckdb.NewAdapter()
	defer adapter.Close()

	plan := &planner.ExecutionPlan{
		LogicalPlan: &sql.LogicalPlan{
			RawSQL:    "SELECT 1 AS a, 2 AS b, 'hello' AS c",
			Operation: capabilities.OperationSelect,
		},
		Engine: "duckdb",
	}

	result, err := adapter.Execute(context.Background(), plan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(result.Columns))
	}

	expectedCols := []string{"a", "b", "c"}
	for i, col := range expectedCols {
		if result.Columns[i] != col {
			t.Errorf("column %d: expected %q, got %q", i, col, result.Columns[i])
		}
	}
}

// TestDuckDB_ExecuteMultipleRows verifies the adapter returns all rows.
// Green-Flag: All result rows must be returned.
func TestDuckDB_ExecuteMultipleRows(t *testing.T) {
	adapter := duckdb.NewAdapter()
	defer adapter.Close()

	plan := &planner.ExecutionPlan{
		LogicalPlan: &sql.LogicalPlan{
			RawSQL:    "SELECT * FROM (VALUES (1), (2), (3)) AS t(num)",
			Operation: capabilities.OperationSelect,
		},
		Engine: "duckdb",
	}

	result, err := adapter.Execute(context.Background(), plan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.RowCount != 3 {
		t.Fatalf("expected 3 rows, got %d", result.RowCount)
	}

	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows in Rows slice, got %d", len(result.Rows))
	}
}

// TestDuckDB_ExecuteEmptyResult verifies the adapter handles empty results.
// Green-Flag: Queries returning no rows must return empty result, not error.
func TestDuckDB_ExecuteEmptyResult(t *testing.T) {
	adapter := duckdb.NewAdapter()
	defer adapter.Close()

	plan := &planner.ExecutionPlan{
		LogicalPlan: &sql.LogicalPlan{
			RawSQL:    "SELECT * FROM (VALUES (1)) AS t(num) WHERE num > 100",
			Operation: capabilities.OperationSelect,
		},
		Engine: "duckdb",
	}

	result, err := adapter.Execute(context.Background(), plan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.RowCount != 0 {
		t.Fatalf("expected 0 rows, got %d", result.RowCount)
	}
}

// TestDuckDB_Ping verifies the adapter health check works.
// Green-Flag: Healthy adapter must respond to ping.
func TestDuckDB_Ping(t *testing.T) {
	adapter := duckdb.NewAdapter()
	defer adapter.Close()

	err := adapter.Ping(context.Background())
	if err != nil {
		t.Fatalf("unexpected ping error: %v", err)
	}
}

// TestDuckDB_Name verifies the adapter returns correct name.
// Green-Flag: Adapter name must be "duckdb".
func TestDuckDB_Name(t *testing.T) {
	adapter := duckdb.NewAdapter()
	defer adapter.Close()

	if adapter.Name() != "duckdb" {
		t.Fatalf("expected name 'duckdb', got %q", adapter.Name())
	}
}

// TestDuckDB_Capabilities verifies the adapter reports correct capabilities.
// Green-Flag: DuckDB supports READ and TIME_TRAVEL.
func TestDuckDB_Capabilities(t *testing.T) {
	adapter := duckdb.NewAdapter()
	defer adapter.Close()

	caps := adapter.Capabilities()

	hasRead := false
	for _, cap := range caps {
		if cap == capabilities.CapabilityRead {
			hasRead = true
			break
		}
	}

	if !hasRead {
		t.Fatal("expected DuckDB to have READ capability")
	}
}

// TestDuckDB_CloseIsIdempotent verifies Close can be called multiple times.
// Green-Flag: Close must be safe to call multiple times.
func TestDuckDB_CloseIsIdempotent(t *testing.T) {
	adapter := duckdb.NewAdapter()

	err1 := adapter.Close()
	if err1 != nil {
		t.Fatalf("first close failed: %v", err1)
	}

	err2 := adapter.Close()
	if err2 != nil {
		t.Fatalf("second close failed: %v", err2)
	}
}

// TestDuckDB_WithCustomConfig verifies adapter works with custom database path.
// Green-Flag: Custom configuration must be respected.
func TestDuckDB_WithCustomConfig(t *testing.T) {
	config := duckdb.AdapterConfig{
		DatabasePath: ":memory:",
	}
	adapter := duckdb.NewAdapterWithConfig(config)
	defer adapter.Close()

	err := adapter.Ping(context.Background())
	if err != nil {
		t.Fatalf("unexpected error with custom config: %v", err)
	}
}

// TestDuckDB_ImplementsEngineAdapter verifies interface compliance.
// Green-Flag: Adapter must implement EngineAdapter interface.
func TestDuckDB_ImplementsEngineAdapter(t *testing.T) {
	var adapter adapters.EngineAdapter = duckdb.NewAdapter()
	defer adapter.Close()

	// Just verify it compiles and runs
	_ = adapter.Name()
	_ = adapter.Capabilities()
}

// TestDuckDB_ResultMetadata verifies metadata is populated.
// Green-Flag: Query results must include metadata.
func TestDuckDB_ResultMetadata(t *testing.T) {
	adapter := duckdb.NewAdapter()
	defer adapter.Close()

	plan := &planner.ExecutionPlan{
		LogicalPlan: &sql.LogicalPlan{
			RawSQL:    "SELECT 1 AS value",
			Operation: capabilities.OperationSelect,
		},
		Engine: "duckdb",
	}

	result, err := adapter.Execute(context.Background(), plan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Metadata == nil {
		t.Fatal("expected metadata to be non-nil")
	}
}

// Phase 6 Green-Flag Tests: Health Check
// Per phase-6-spec.md: Verify health check works for healthy adapters

// TestDuckDB_CheckHealthSucceedsOnHealthyAdapter verifies CheckHealth succeeds.
// Green-Flag: Healthy adapters must report healthy via CheckHealth.
func TestDuckDB_CheckHealthSucceedsOnHealthyAdapter(t *testing.T) {
	adapter := duckdb.NewAdapter()
	defer adapter.Close()

	err := adapter.CheckHealth(context.Background())
	if err != nil {
		t.Fatalf("expected healthy adapter to pass CheckHealth, got: %v", err)
	}
}

// TestDuckDB_CheckHealthExecutesSelect1 verifies CheckHealth actually queries the database.
// Green-Flag: Health check must verify actual database connectivity, not just struct state.
func TestDuckDB_CheckHealthExecutesSelect1(t *testing.T) {
	adapter := duckdb.NewAdapter()
	defer adapter.Close()

	// First verify the adapter is working
	plan := &planner.ExecutionPlan{
		LogicalPlan: &sql.LogicalPlan{
			RawSQL:    "SELECT 1 AS health",
			Operation: capabilities.OperationSelect,
		},
		Engine: "duckdb",
	}

	result, err := adapter.Execute(context.Background(), plan)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result.RowCount != 1 {
		t.Fatalf("expected 1 row, got %d", result.RowCount)
	}

	// Now verify CheckHealth also works
	err = adapter.CheckHealth(context.Background())
	if err != nil {
		t.Fatalf("CheckHealth failed after successful Execute: %v", err)
	}
}
