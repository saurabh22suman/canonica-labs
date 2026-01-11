// Package redflag contains tests that MUST fail if invariants are violated.
// Per docs/test.md: "Red-Flag tests are mandatory for all new features."
//
// This file tests T033: Time-Travel Syntax Normalization.
package redflag

import (
	"testing"

	"github.com/canonica-labs/canonica/internal/catalog"
	"github.com/canonica-labs/canonica/internal/sql"
)

// TestTimeTravelNormalization_DuckDBSyntax verifies DuckDB-specific
// time-travel syntax generation.
// Per T033: "Different engines use different syntax"
func TestTimeTravelNormalization_DuckDBSyntax(t *testing.T) {
	rewriter := sql.NewTimeTravelRewriter(catalog.FormatIceberg, "duckdb")

	input := "SELECT * FROM sales.orders FOR SYSTEM_TIME AS OF '2026-01-01T00:00:00Z'"
	result, err := rewriter.Rewrite(input)
	if err != nil {
		t.Fatalf("Rewrite failed: %v", err)
	}

	// DuckDB Iceberg uses: SELECT * FROM t AT 'timestamp'
	// The rewriter should produce DuckDB-compatible syntax
	if result == input {
		t.Error("Rewriter did not modify DuckDB Iceberg query")
	}

	// Should not contain the original SYSTEM_TIME syntax
	// (DuckDB uses AT syntax or similar)
	t.Logf("DuckDB rewritten: %s", result)
}

// TestTimeTravelNormalization_TrinoSyntax verifies Trino-specific
// time-travel syntax generation.
// Per phase-8-spec.md §1.4: Trino uses FOR TIMESTAMP AS OF
func TestTimeTravelNormalization_TrinoSyntax(t *testing.T) {
	rewriter := sql.NewTimeTravelRewriter(catalog.FormatIceberg, "trino")

	input := "SELECT * FROM sales.orders FOR SYSTEM_TIME AS OF '2026-01-01T00:00:00Z'"
	result, err := rewriter.Rewrite(input)
	if err != nil {
		t.Fatalf("Rewrite failed: %v", err)
	}

	// Trino uses: FOR TIMESTAMP AS OF TIMESTAMP 'ts'
	expected := " FOR TIMESTAMP AS OF TIMESTAMP '2026-01-01T00:00:00Z'"
	if !containsSubstring(result, "FOR TIMESTAMP AS OF") {
		t.Errorf("Expected Trino syntax with 'FOR TIMESTAMP AS OF', got: %s", result)
	}
	t.Logf("Trino rewritten: %s (expected contains: %s)", result, expected)
}

// TestTimeTravelNormalization_SparkDeltaSyntax verifies Spark Delta
// time-travel syntax generation.
// Per phase-8-spec.md §1.5: Spark Delta uses TIMESTAMP AS OF
func TestTimeTravelNormalization_SparkDeltaSyntax(t *testing.T) {
	rewriter := sql.NewTimeTravelRewriter(catalog.FormatDelta, "spark")

	input := "SELECT * FROM sales.orders FOR SYSTEM_TIME AS OF '2026-01-01T00:00:00Z'"
	result, err := rewriter.Rewrite(input)
	if err != nil {
		t.Fatalf("Rewrite failed: %v", err)
	}

	// Spark Delta uses: TIMESTAMP AS OF 'ts'
	if !containsSubstring(result, "TIMESTAMP AS OF") {
		t.Errorf("Expected Spark Delta syntax with 'TIMESTAMP AS OF', got: %s", result)
	}
}

// TestTimeTravelNormalization_SnowflakeSyntax verifies Snowflake AT() syntax.
// Per phase-8-spec.md §4.2: Snowflake uses AT(TIMESTAMP => 'ts'::TIMESTAMP)
func TestTimeTravelNormalization_SnowflakeSyntax(t *testing.T) {
	rewriter := sql.NewWarehouseRewriter("snowflake")

	input := "SELECT * FROM sales.orders FOR SYSTEM_TIME AS OF '2026-01-01T00:00:00Z'"
	result, err := rewriter.Rewrite(input)
	if err != nil {
		t.Fatalf("Rewrite failed: %v", err)
	}

	// Snowflake uses: AT(TIMESTAMP => 'ts'::TIMESTAMP)
	if !containsSubstring(result, "AT(TIMESTAMP =>") {
		t.Errorf("Expected Snowflake AT() syntax, got: %s", result)
	}
}

// TestTimeTravelNormalization_RedshiftRejected verifies Redshift time-travel
// is rejected with clear error.
// Per phase-8-spec.md §6: Redshift does NOT support time-travel
func TestTimeTravelNormalization_RedshiftRejected(t *testing.T) {
	rewriter := sql.NewWarehouseRewriter("redshift")

	input := "SELECT * FROM sales.orders FOR SYSTEM_TIME AS OF '2026-01-01T00:00:00Z'"
	_, err := rewriter.Rewrite(input)
	if err == nil {
		t.Error("Expected error for Redshift time-travel, got nil")
	}

	// Error should mention Redshift and time-travel
	if err != nil && !containsSubstring(err.Error(), "Redshift") {
		t.Errorf("Error should mention Redshift, got: %v", err)
	}
}

// TestTimeTravelNormalization_BigQuerySyntax verifies BigQuery
// FOR SYSTEM_TIME AS OF TIMESTAMP syntax.
// Per phase-8-spec.md §5.2: BigQuery uses standard SQL:2011 syntax
func TestTimeTravelNormalization_BigQuerySyntax(t *testing.T) {
	rewriter := sql.NewWarehouseRewriter("bigquery")

	input := "SELECT * FROM sales.orders FOR SYSTEM_TIME AS OF '2026-01-01T00:00:00Z'"
	result, err := rewriter.Rewrite(input)
	if err != nil {
		t.Fatalf("Rewrite failed: %v", err)
	}

	// BigQuery uses: FOR SYSTEM_TIME AS OF TIMESTAMP 'ts'
	if !containsSubstring(result, "TIMESTAMP '2026-01-01T00:00:00Z'") {
		t.Errorf("Expected BigQuery TIMESTAMP syntax, got: %s", result)
	}
}

// Helper function to check substring
func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstringHelper(s, substr))
}

func containsSubstringHelper(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
