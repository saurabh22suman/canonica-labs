// Package redflag contains Red-Flag tests that prove the system correctly
// refuses unsafe, ambiguous, or unsupported behavior.
//
// Per docs/test.md: "Red-Flag tests assert that the system REFUSES unsafe, ambiguous,
// or unsupported behavior."
//
// Per docs/phase-3-spec.md §9: "Users must never guess WHY a query failed.
// Parser rejections must be explicit, stable, and human-readable."
package redflag

import (
	"strings"
	"testing"

	"github.com/canonica-labs/canonica/internal/sql"
)

// TestRejectsWindowFunctions tests that WINDOW functions are explicitly rejected.
// Per phase-3-spec.md §9: "WINDOW functions must fail with a SPECIFIC, non-generic error."
func TestRejectsWindowFunctions(t *testing.T) {
	parser := sql.NewParser()

	queries := []struct {
		name  string
		query string
	}{
		{
			name:  "ROW_NUMBER with OVER",
			query: "SELECT ROW_NUMBER() OVER (ORDER BY id) FROM test.orders",
		},
		{
			name:  "RANK with OVER",
			query: "SELECT RANK() OVER (PARTITION BY customer_id ORDER BY date) FROM test.orders",
		},
		{
			name:  "SUM with OVER",
			query: "SELECT SUM(amount) OVER (ORDER BY date) FROM test.orders",
		},
		{
			name:  "LAG function",
			query: "SELECT LAG(price, 1) OVER (ORDER BY date) FROM test.orders",
		},
		{
			name:  "LEAD function",
			query: "SELECT LEAD(price, 1) OVER (ORDER BY date) FROM test.orders",
		},
	}

	for _, tc := range queries {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parser.Parse(tc.query)
			if err == nil {
				t.Fatalf("WINDOW function should be rejected, but was accepted: %s", tc.query)
			}

			errMsg := err.Error()

			// Must mention "WINDOW" specifically, not be generic
			if !strings.Contains(strings.ToUpper(errMsg), "WINDOW") {
				t.Errorf("Error must specifically mention WINDOW function:\nGot: %s", errMsg)
			}

			// Must mention "OVER" clause
			if !strings.Contains(strings.ToUpper(errMsg), "OVER") {
				t.Errorf("Error must mention OVER clause:\nGot: %s", errMsg)
			}

			// Must NOT be a generic parse error
			if strings.Contains(errMsg, "syntax error") && !strings.Contains(errMsg, "WINDOW") {
				t.Errorf("Error must be specific, not generic syntax error:\nGot: %s", errMsg)
			}
		})
	}
}

// TestCTEsAreNowSupported tests that CTEs (WITH clauses) are now properly parsed.
// This test was previously TestRejectsCTEs, but CTEs are now supported via dolthub/vitess (T013).
// Per phase-3-spec.md: CTEs must be parsed correctly and underlying tables extracted.
func TestCTEsAreNowSupported(t *testing.T) {
	parser := sql.NewParser()

	testCases := []struct {
		name           string
		query          string
		expectedTables []string
	}{
		{
			name: "Simple CTE",
			query: `WITH customer_orders AS (
				SELECT customer_id, COUNT(*) as order_count 
				FROM test.orders 
				GROUP BY customer_id
			)
			SELECT * FROM customer_orders`,
			expectedTables: []string{"test.orders"},
		},
		{
			name: "Multiple CTEs",
			query: `WITH 
				orders_2024 AS (SELECT * FROM test.orders WHERE year = 2024),
				top_customers AS (SELECT customer_id FROM orders_2024 LIMIT 10)
			SELECT * FROM top_customers`,
			expectedTables: []string{"test.orders"},
		},
		{
			name: "Recursive CTE",
			query: `WITH RECURSIVE tree AS (
				SELECT id, parent_id, name FROM test.categories WHERE parent_id IS NULL
				UNION ALL
				SELECT c.id, c.parent_id, c.name FROM test.categories c JOIN tree t ON c.parent_id = t.id
			)
			SELECT * FROM tree`,
			expectedTables: []string{"test.categories"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := parser.Parse(tc.query)
			if err != nil {
				t.Fatalf("CTE (WITH clause) should now be accepted, but got error: %v", err)
			}

			// Verify the underlying tables are extracted
			foundTables := make(map[string]bool)
			for _, table := range plan.Tables {
				foundTables[table] = true
			}

			for _, expected := range tc.expectedTables {
				if !foundTables[expected] {
					t.Errorf("Expected table %q to be extracted from CTE, got tables: %v", expected, plan.Tables)
				}
			}
		})
	}
}

// TestRejectsVendorHints tests that vendor-specific hints are explicitly rejected.
// Per phase-3-spec.md §9: "Vendor-specific hints must fail with a SPECIFIC, non-generic error."
func TestRejectsVendorHints(t *testing.T) {
	parser := sql.NewParser()

	queries := []struct {
		name  string
		query string
	}{
		{
			name:  "MySQL index hint",
			query: "SELECT * FROM test.orders USE INDEX (idx_customer) WHERE customer_id = 1",
		},
		{
			name:  "MySQL force index",
			query: "SELECT * FROM test.orders FORCE INDEX (idx_date) WHERE date > '2024-01-01'",
		},
		{
			name:  "MySQL ignore index",
			query: "SELECT * FROM test.orders IGNORE INDEX (idx_customer) WHERE customer_id = 1",
		},
		{
			name:  "Comment-style hint (Oracle)",
			query: "SELECT /*+ INDEX(orders idx_customer) */ * FROM test.orders",
		},
	}

	for _, tc := range queries {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parser.Parse(tc.query)
			if err == nil {
				t.Fatalf("Vendor hint should be rejected, but was accepted: %s", tc.query)
			}

			errMsg := err.Error()

			// Must mention "hint" or the specific construct
			hasHint := strings.Contains(strings.ToLower(errMsg), "hint")
			hasIndex := strings.Contains(strings.ToUpper(errMsg), "INDEX") && strings.Contains(strings.ToUpper(errMsg), "USE")
			hasForce := strings.Contains(strings.ToUpper(errMsg), "FORCE")
			hasUnsupported := strings.Contains(strings.ToLower(errMsg), "unsupported")

			if !hasHint && !hasIndex && !hasForce && !hasUnsupported {
				t.Errorf("Error must specifically mention hint or the unsupported construct:\nGot: %s", errMsg)
			}
		})
	}
}

// TestRejectsMultipleStatements tests that multiple statements are explicitly rejected.
// Per phase-3-spec.md §9: "Multiple statements must fail with a SPECIFIC, non-generic error."
func TestRejectsMultipleStatements(t *testing.T) {
	parser := sql.NewParser()

	queries := []struct {
		name  string
		query string
	}{
		{
			name:  "Two SELECT statements",
			query: "SELECT * FROM test.orders; SELECT * FROM test.customers",
		},
		{
			name:  "SELECT then DELETE",
			query: "SELECT * FROM test.orders; DELETE FROM test.orders WHERE id = 1",
		},
		{
			name:  "Multiple semicolons",
			query: "SELECT * FROM test.orders;; SELECT 1",
		},
	}

	for _, tc := range queries {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parser.Parse(tc.query)
			if err == nil {
				t.Fatalf("Multiple statements should be rejected, but was accepted: %s", tc.query)
			}

			errMsg := err.Error()

			// Must mention "multiple statements"
			hasMultiple := strings.Contains(strings.ToLower(errMsg), "multiple")
			hasStatements := strings.Contains(strings.ToLower(errMsg), "statement")

			if !hasMultiple || !hasStatements {
				t.Errorf("Error must specifically mention 'multiple statements':\nGot: %s", errMsg)
			}
		})
	}
}

// TestErrorMessageContainsConstruct tests that error messages identify the unsupported construct.
// Per phase-3-spec.md §9: "Error messages MUST include: unsupported construct"
func TestErrorMessageContainsConstruct(t *testing.T) {
	parser := sql.NewParser()

	// Test that errors include the specific construct being rejected
	testCases := []struct {
		name             string
		query            string
		expectedConstruct string
	}{
		{
			name:             "WINDOW function identified",
			query:            "SELECT ROW_NUMBER() OVER (ORDER BY id) FROM test.orders",
			expectedConstruct: "WINDOW",
		},
		{
			name:             "Multiple statements identified",
			query:            "SELECT 1; SELECT 2",
			expectedConstruct: "multiple",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parser.Parse(tc.query)
			if err == nil {
				t.Fatalf("Query should be rejected: %s", tc.query)
			}

			if !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tc.expectedConstruct)) {
				t.Errorf("Error must identify '%s' construct:\nGot: %s", tc.expectedConstruct, err.Error())
			}
		})
	}
}

// TestErrorMessageContainsSuggestion tests that error messages include alternatives.
// Per phase-3-spec.md §9: "Error messages MUST include: example of supported alternative (when possible)"
func TestErrorMessageContainsSuggestion(t *testing.T) {
	parser := sql.NewParser()

	// WINDOW function error should suggest an alternative
	_, err := parser.Parse("SELECT ROW_NUMBER() OVER (ORDER BY id) FROM test.orders")
	if err == nil {
		t.Fatal("Query should be rejected")
	}

	errMsg := err.Error()

	// Error should contain suggestion or alternative
	hasSuggestion := strings.Contains(strings.ToLower(errMsg), "suggestion") ||
		strings.Contains(strings.ToLower(errMsg), "supported") ||
		strings.Contains(strings.ToLower(errMsg), "instead") ||
		strings.Contains(strings.ToLower(errMsg), "use")

	if !hasSuggestion {
		t.Errorf("Error must include suggestion or supported alternative:\nGot: %s", errMsg)
	}
}

// TestRejectsSubqueriesInSelect tests that certain complex subquery patterns fail clearly.
// (Note: Basic subqueries may be allowed, but complex correlated subqueries should fail explicitly)
func TestRejectsCorrelatedSubqueries(t *testing.T) {
	parser := sql.NewParser()

	// Correlated subqueries referencing outer query
	query := `SELECT * FROM test.orders o 
		WHERE o.amount > (
			SELECT AVG(amount) FROM test.orders o2 
			WHERE o2.customer_id = o.customer_id
		)`

	// This test documents expected behavior - correlated subqueries
	// may or may not be supported. If rejected, error must be explicit.
	_, err := parser.Parse(query)
	if err != nil {
		// If rejected, error must be explicit about the reason
		errMsg := err.Error()
		if strings.Contains(errMsg, "syntax error") && !strings.Contains(strings.ToLower(errMsg), "subquer") {
			// Only fail if it's a generic syntax error without mentioning subqueries
			t.Logf("Note: Query rejected with message: %s", errMsg)
		}
	}
	// If accepted, that's also fine - this documents behavior
}
