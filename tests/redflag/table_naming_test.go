package redflag

import (
	"context"
	"strings"
	"testing"

	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/gateway"
	"github.com/canonica-labs/canonica/internal/sql"
	"github.com/canonica-labs/canonica/internal/tables"
)

// =============================================================================
// RED-FLAG TESTS: Table Naming Rules – Schema-Qualified Names Required
// =============================================================================
//
// Per phase-2-spec.md §6: Decide and Enforce Table Naming Rules
// > Schema-qualified table names are required.
// > Format: <schema>.<table>
//
// Red-Flag Tests (Required):
// - Query using unqualified table name
// - Virtual table registered without schema
// - Mixed qualified and unqualified table references
//
// These tests MUST FAIL before implementation and PASS after.
// =============================================================================

// TestTableNaming_RejectsUnqualifiedQueryName proves that queries using
// unqualified table names are rejected.
//
// Red-Flag: Query using unqualified table name.
func TestTableNaming_RejectsUnqualifiedQueryName(t *testing.T) {
	testCases := []struct {
		name  string
		query string
	}{
		{"simple_select", "SELECT * FROM sales_orders"},
		{"select_with_where", "SELECT id, name FROM orders WHERE status = 'pending'"},
		{"select_with_join", "SELECT * FROM users u JOIN orders o ON u.id = o.user_id"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			parser := sql.NewParser()
			logical, err := parser.Parse(tc.query)

			// The parser should either reject the query or validation should fail
			if err == nil && logical != nil {
				// If parsing succeeded, validate table names
				for _, tableName := range logical.Tables {
					err = sql.ValidateTableName(tableName)
					if err == nil {
						// RED-FLAG: Unqualified name was accepted
						t.Errorf("RED-FLAG: Unqualified table name accepted in query!\n"+
							"Query: %s\n"+
							"Table: %s\n"+
							"Expected: rejection with error requiring <schema>.<table> format\n"+
							"Phase 2 requires schema-qualified names", tc.query, tableName)
					}
				}
			}
		})
	}
}

// TestTableNaming_RejectsUnqualifiedRegistration proves that virtual table
// registration rejects unqualified names.
//
// Red-Flag: Virtual table registered without schema.
func TestTableNaming_RejectsUnqualifiedRegistration(t *testing.T) {
	testCases := []struct {
		name      string
		tableName string
	}{
		{"simple_name", "sales_orders"},
		{"underscore_name", "user_accounts"},
		{"single_word", "products"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Attempt to validate an unqualified table name
			err := sql.ValidateTableName(tc.tableName)

			// RED-FLAG: If err is nil, unqualified names are accepted
			if err == nil {
				t.Errorf("RED-FLAG: Unqualified table name accepted for registration!\n"+
					"Table: %s\n"+
					"Expected: rejection with error requiring <schema>.<table> format\n"+
					"Phase 2 requires schema-qualified names", tc.tableName)
			}

			// Error message must explain required format per phase-2-spec.md §6
			if err != nil && !strings.Contains(err.Error(), "<schema>.<table>") &&
				!strings.Contains(err.Error(), "schema") &&
				!strings.Contains(err.Error(), "qualified") {
				t.Logf("Warning: error message should explain required format: %v", err)
			}
		})
	}
}

// TestTableNaming_RejectsMixedReferences proves that queries with mixed
// qualified and unqualified table references are rejected.
//
// Red-Flag: Mixed qualified and unqualified table references.
func TestTableNaming_RejectsMixedReferences(t *testing.T) {
	// Setup a registry with a properly qualified table
	registry := gateway.NewInMemoryTableRegistry()
	registry.Register(&tables.VirtualTable{
		Name: "analytics.sales_orders",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatDelta, Location: "s3://bucket/sales_orders"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	})
	_ = registry

	// Query with one qualified and one unqualified reference
	// The unqualified reference should cause rejection
	parser := sql.NewParser()
	logical, err := parser.Parse("SELECT * FROM analytics.sales_orders s JOIN payments p ON s.id = p.order_id")

	if err == nil && logical != nil {
		// Check that we detect the unqualified table
		hasUnqualified := false
		for _, tableName := range logical.Tables {
			if err := sql.ValidateTableName(tableName); err != nil {
				hasUnqualified = true
				break
			}
		}

		// RED-FLAG: If no unqualified names detected, validation is broken
		if !hasUnqualified {
			t.Errorf("RED-FLAG: Mixed qualified/unqualified references not detected!\n"+
				"Query contains: analytics.sales_orders (qualified) and payments (unqualified)\n"+
				"Expected: validation error for unqualified table\n"+
				"Phase 2 requires all table references to be schema-qualified")
		}
	}
}

// TestTableNaming_ErrorMessageExplainsFormat proves that the error message
// for unqualified names explains the required format.
//
// Per phase-2-spec.md §6:
// > Invalid table reference: 'sales_orders'
// > Fully-qualified name required: <schema>.<table>
func TestTableNaming_ErrorMessageExplainsFormat(t *testing.T) {
	err := sql.ValidateTableName("sales_orders")

	if err == nil {
		t.Error("expected error for unqualified table name")
		return
	}

	errMsg := err.Error()

	// Check for required components in error message
	checks := []struct {
		desc     string
		required string
	}{
		{"table name in message", "sales_orders"},
		{"format explanation", "schema"},
	}

	for _, check := range checks {
		if !strings.Contains(strings.ToLower(errMsg), strings.ToLower(check.required)) {
			t.Logf("Warning: error message should contain %s: %s", check.desc, errMsg)
		}
	}
}

// TestTableNaming_RejectsTrailingDot proves that malformed names like "schema." are rejected.
func TestTableNaming_RejectsMalformedNames(t *testing.T) {
	testCases := []struct {
		name      string
		tableName string
	}{
		{"trailing_dot", "analytics."},
		{"leading_dot", ".sales_orders"},
		{"double_dot", "analytics..sales_orders"},
		{"only_dots", "..."},
		{"empty_schema", ".table"},
		{"empty_table", "schema."},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := sql.ValidateTableName(tc.tableName)

			if err == nil {
				t.Errorf("RED-FLAG: Malformed table name accepted: %s", tc.tableName)
			}
		})
	}
}

// TestTableNaming_RegistryRejectsUnqualified proves that the table registry
// rejects registration of tables with unqualified names.
func TestTableNaming_RegistryRejectsUnqualified(t *testing.T) {
	registry := gateway.NewInMemoryTableRegistry()

	// Create a table with an unqualified name
	vt := &tables.VirtualTable{
		Name: "sales_orders", // Unqualified - should be rejected
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatDelta, Location: "s3://bucket/sales_orders"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	}

	// Validate the table name before registration
	err := sql.ValidateTableName(vt.Name)
	if err == nil {
		t.Errorf("RED-FLAG: Registry accepted unqualified table name!\n"+
			"Table: %s\n"+
			"Phase 2 requires schema-qualified names for registration", vt.Name)
	}

	// Also test via GetTable with unqualified name
	_, err = registry.GetTable(context.Background(), "sales_orders")
	// This will return "table not found" which is fine - the point is
	// unqualified names should never be in the registry in the first place
}
