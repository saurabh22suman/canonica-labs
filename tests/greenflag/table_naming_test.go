package greenflag

import (
	"context"
	"testing"

	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/gateway"
	"github.com/canonica-labs/canonica/internal/sql"
	"github.com/canonica-labs/canonica/internal/tables"
)

// =============================================================================
// GREEN-FLAG TESTS: Table Naming Rules – Schema-Qualified Names
// =============================================================================
//
// Per phase-2-spec.md §6: Green-Flag Tests (Required)
// - Fully-qualified table names resolve correctly
// - Authorization works with qualified names
// - Multi-schema queries resolve deterministically
//
// These tests verify expected behavior for VALID table naming scenarios.
// =============================================================================

// TestTableNaming_ValidQualifiedNames proves that properly qualified
// table names are accepted.
//
// Green-Flag: Fully-qualified table names resolve correctly.
func TestTableNaming_ValidQualifiedNames(t *testing.T) {
	validNames := []string{
		"analytics.sales_orders",
		"dw.fact_transactions",
		"raw.events",
		"staging.user_activity",
		"public.customers",
		"finance.payments",
		"marketing.campaigns",
	}

	for _, name := range validNames {
		t.Run(name, func(t *testing.T) {
			err := sql.ValidateTableName(name)
			if err != nil {
				t.Errorf("GREEN-FLAG VIOLATION: Valid qualified name rejected!\n"+
					"Table: %s\n"+
					"Error: %v", name, err)
			}
		})
	}
}

// TestTableNaming_QualifiedNamesResolveInQuery proves that queries with
// qualified table names are parsed correctly.
//
// Green-Flag: Fully-qualified table names resolve correctly.
func TestTableNaming_QualifiedNamesResolveInQuery(t *testing.T) {
	testCases := []struct {
		name     string
		query    string
		expected []string
	}{
		{
			name:     "simple_select",
			query:    "SELECT * FROM analytics.sales_orders",
			expected: []string{"analytics.sales_orders"},
		},
		{
			name:     "select_with_alias",
			query:    "SELECT * FROM dw.fact_transactions ft WHERE ft.amount > 100",
			expected: []string{"dw.fact_transactions"},
		},
		{
			name:     "join_qualified",
			query:    "SELECT * FROM analytics.orders o JOIN analytics.customers c ON o.customer_id = c.id",
			expected: []string{"analytics.orders", "analytics.customers"},
		},
		{
			name:     "multi_schema_join",
			query:    "SELECT * FROM analytics.sales s JOIN finance.payments p ON s.id = p.sale_id",
			expected: []string{"analytics.sales", "finance.payments"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			parser := sql.NewParser()
			logical, err := parser.Parse(tc.query)
			if err != nil {
				t.Fatalf("failed to parse query: %v", err)
			}

			// Validate all extracted table names
			for _, tableName := range logical.Tables {
				if err := sql.ValidateTableName(tableName); err != nil {
					t.Errorf("GREEN-FLAG VIOLATION: Query with qualified names failed validation!\n"+
						"Query: %s\n"+
						"Table: %s\n"+
						"Error: %v", tc.query, tableName, err)
				}
			}

			// Check that expected tables are present
			for _, expected := range tc.expected {
				found := false
				for _, actual := range logical.Tables {
					if actual == expected {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("expected table %s not found in parsed tables: %v", expected, logical.Tables)
				}
			}
		})
	}
}

// TestTableNaming_RegistryAcceptsQualified proves that the table registry
// accepts properly qualified table names.
//
// Green-Flag: Authorization works with qualified names.
func TestTableNaming_RegistryAcceptsQualified(t *testing.T) {
	registry := gateway.NewInMemoryTableRegistry()

	// Register a table with a qualified name
	vt := &tables.VirtualTable{
		Name: "analytics.sales_orders",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatDelta, Location: "s3://bucket/sales_orders"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	}

	// Validate the name first
	if err := sql.ValidateTableName(vt.Name); err != nil {
		t.Fatalf("qualified name should be valid: %v", err)
	}

	// Register and retrieve
	registry.Register(vt)

	retrieved, err := registry.GetTable(context.Background(), "analytics.sales_orders")
	if err != nil {
		t.Errorf("GREEN-FLAG VIOLATION: Failed to retrieve registered table!\n"+
			"Table: %s\n"+
			"Error: %v", vt.Name, err)
	}

	if retrieved == nil || retrieved.Name != vt.Name {
		t.Errorf("retrieved table doesn't match registered table")
	}
}

// TestTableNaming_MultiSchemaResolvesDeterministically proves that queries
// across multiple schemas resolve correctly without ambiguity.
//
// Green-Flag: Multi-schema queries resolve deterministically.
func TestTableNaming_MultiSchemaResolvesDeterministically(t *testing.T) {
	registry := gateway.NewInMemoryTableRegistry()

	// Register tables with same base name but different schemas
	registry.Register(&tables.VirtualTable{
		Name: "analytics.orders",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatDelta, Location: "s3://bucket/analytics/orders"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	})
	registry.Register(&tables.VirtualTable{
		Name: "staging.orders",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatDelta, Location: "s3://bucket/staging/orders"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	})

	// Query that references both
	parser := sql.NewParser()
	logical, err := parser.Parse("SELECT * FROM analytics.orders a JOIN staging.orders s ON a.id = s.id")
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	// Verify both tables are resolved correctly
	analyticsFound := false
	stagingFound := false
	for _, table := range logical.Tables {
		if table == "analytics.orders" {
			analyticsFound = true
		}
		if table == "staging.orders" {
			stagingFound = true
		}
	}

	if !analyticsFound {
		t.Error("analytics.orders not found in parsed tables")
	}
	if !stagingFound {
		t.Error("staging.orders not found in parsed tables")
	}

	// Verify we can retrieve each table deterministically
	analytics, err := registry.GetTable(context.Background(), "analytics.orders")
	if err != nil {
		t.Errorf("failed to get analytics.orders: %v", err)
	}
	if analytics.Sources[0].Location != "s3://bucket/analytics/orders" {
		t.Error("analytics.orders has wrong source")
	}

	staging, err := registry.GetTable(context.Background(), "staging.orders")
	if err != nil {
		t.Errorf("failed to get staging.orders: %v", err)
	}
	if staging.Sources[0].Location != "s3://bucket/staging/orders" {
		t.Error("staging.orders has wrong source")
	}
}

// TestTableNaming_IsQualifiedHelper proves the helper function works.
func TestTableNaming_IsQualifiedHelper(t *testing.T) {
	qualified := []string{
		"schema.table",
		"analytics.sales",
		"dw.fact_orders",
	}

	unqualified := []string{
		"sales",
		"orders",
		"",
		".",
		".table",
		"schema.",
	}

	for _, name := range qualified {
		if !sql.IsQualifiedTableName(name) {
			t.Errorf("expected %s to be qualified", name)
		}
	}

	for _, name := range unqualified {
		if sql.IsQualifiedTableName(name) {
			t.Errorf("expected %s to be unqualified", name)
		}
	}
}
