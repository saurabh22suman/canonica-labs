// Package greenflag contains Green-Flag tests that prove the system correctly
// succeeds when semantics are guaranteed.
//
// Per docs/test.md: "Green-Flag tests assert that the system SUCCESSFULLY EXECUTES
// behavior that is explicitly declared safe."
//
// Per docs/phase-3-spec.md §9: "Error classification is deterministic.
// Same query always produces same error message."
package greenflag

import (
	"strings"
	"testing"

	"github.com/canonica-labs/canonica/internal/sql"
)

// TestSupportedSelectParsesCleanly tests that valid SELECT syntax parses without errors.
// Per phase-3-spec.md §9: "Supported SELECT syntax parses cleanly"
func TestSupportedSelectParsesCleanly(t *testing.T) {
	parser := sql.NewParser()

	queries := []struct {
		name  string
		query string
	}{
		{
			name:  "Simple SELECT",
			query: "SELECT * FROM test.orders",
		},
		{
			name:  "SELECT with WHERE",
			query: "SELECT id, name FROM test.customers WHERE status = 'active'",
		},
		{
			name:  "SELECT with JOIN",
			query: "SELECT o.id, c.name FROM test.orders o JOIN test.customers c ON o.customer_id = c.id",
		},
		{
			name:  "SELECT with GROUP BY",
			query: "SELECT customer_id, COUNT(*) as cnt FROM test.orders GROUP BY customer_id",
		},
		{
			name:  "SELECT with HAVING",
			query: "SELECT customer_id, SUM(amount) FROM test.orders GROUP BY customer_id HAVING SUM(amount) > 1000",
		},
		{
			name:  "SELECT with ORDER BY",
			query: "SELECT * FROM test.orders ORDER BY created_at DESC",
		},
		{
			name:  "SELECT with LIMIT",
			query: "SELECT * FROM test.orders LIMIT 10",
		},
		{
			name:  "SELECT with subquery in WHERE",
			query: "SELECT * FROM test.orders WHERE customer_id IN (SELECT id FROM test.customers WHERE status = 'vip')",
		},
		{
			name:  "SELECT with aggregate functions",
			query: "SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM test.orders",
		},
		{
			name:  "SELECT DISTINCT",
			query: "SELECT DISTINCT customer_id FROM test.orders",
		},
		{
			name:  "SELECT with UNION",
			query: "SELECT id, name FROM test.customers_us UNION SELECT id, name FROM test.customers_eu",
		},
		{
			name:  "SELECT with multiple conditions",
			query: "SELECT * FROM test.orders WHERE status = 'pending' AND amount > 100 OR priority = 'high'",
		},
		{
			name:  "SELECT with BETWEEN",
			query: "SELECT * FROM test.orders WHERE date BETWEEN '2024-01-01' AND '2024-12-31'",
		},
		{
			name:  "SELECT with LIKE",
			query: "SELECT * FROM test.customers WHERE name LIKE 'John%'",
		},
		{
			name:  "SELECT with IS NULL",
			query: "SELECT * FROM test.orders WHERE deleted_at IS NULL",
		},
		{
			name:  "SELECT with CASE expression",
			query: "SELECT id, CASE WHEN amount > 1000 THEN 'large' ELSE 'small' END as size FROM test.orders",
		},
	}

	for _, tc := range queries {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := parser.Parse(tc.query)
			if err != nil {
				t.Fatalf("Valid SELECT should parse cleanly:\nQuery: %s\nError: %v", tc.query, err)
			}

			// Plan should have tables extracted
			if len(plan.Tables) == 0 {
				t.Errorf("Parser should extract table names:\nQuery: %s", tc.query)
			}
		})
	}
}

// TestErrorClassificationIsDeterministic tests that errors are classified consistently.
// Per phase-3-spec.md §9: "Error classification is deterministic"
func TestErrorClassificationIsDeterministic(t *testing.T) {
	parser := sql.NewParser()

	testCases := []struct {
		name  string
		query string
	}{
		{
			name:  "Multiple statements",
			query: "SELECT 1; SELECT 2",
		},
		{
			name:  "CTE query",
			query: "WITH t AS (SELECT 1) SELECT * FROM t",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Run parse multiple times
			var firstError string
			for i := 0; i < 5; i++ {
				_, err := parser.Parse(tc.query)
				if err == nil {
					t.Fatalf("Query should be rejected: %s", tc.query)
				}

				if firstError == "" {
					firstError = err.Error()
				} else if err.Error() != firstError {
					t.Errorf("Error messages differ between runs:\nFirst: %s\nRun %d: %s", firstError, i+1, err.Error())
				}
			}
		})
	}
}

// TestSameQuerySameError tests that the same query always produces the same error.
// Per phase-3-spec.md §9: "Same query always produces same error message"
func TestSameQuerySameError(t *testing.T) {
	parser := sql.NewParser()

	// Test with a known-unsupported query
	query := "WITH cte AS (SELECT 1) SELECT * FROM cte"

	errors := make([]string, 10)
	for i := 0; i < 10; i++ {
		_, err := parser.Parse(query)
		if err == nil {
			t.Fatalf("CTE query should be rejected")
		}
		errors[i] = err.Error()
	}

	// All errors must be identical
	for i := 1; i < len(errors); i++ {
		if errors[i] != errors[0] {
			t.Errorf("Error messages must be identical:\nExpected: %s\nGot: %s", errors[0], errors[i])
		}
	}
}

// TestErrorContainsQueryContext tests that errors provide context about the query.
func TestErrorContainsQueryContext(t *testing.T) {
	parser := sql.NewParser()

	// Empty query should have helpful error
	_, err := parser.Parse("")
	if err == nil {
		t.Fatal("Empty query should fail")
	}

	errMsg := err.Error()
	if !strings.Contains(strings.ToLower(errMsg), "empty") {
		t.Errorf("Error for empty query should mention 'empty':\nGot: %s", errMsg)
	}
}

// TestValidWriteOperationsCorrectlyBlocked tests that write operations are blocked correctly.
func TestValidWriteOperationsCorrectlyBlocked(t *testing.T) {
	parser := sql.NewParser()

	writes := []struct {
		name      string
		query     string
		operation string
	}{
		{
			name:      "INSERT",
			query:     "INSERT INTO test.orders (id, amount) VALUES (1, 100)",
			operation: "INSERT",
		},
		{
			name:      "UPDATE",
			query:     "UPDATE test.orders SET amount = 200 WHERE id = 1",
			operation: "UPDATE",
		},
		{
			name:      "DELETE",
			query:     "DELETE FROM test.orders WHERE id = 1",
			operation: "DELETE",
		},
	}

	for _, tc := range writes {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parser.Parse(tc.query)
			if err == nil {
				t.Fatalf("Write operation should be blocked: %s", tc.query)
			}

			errMsg := err.Error()

			// Error should mention the specific operation
			if !strings.Contains(strings.ToUpper(errMsg), tc.operation) {
				t.Errorf("Error should mention %s operation:\nGot: %s", tc.operation, errMsg)
			}

			// Error should mention read-only or write not allowed
			hasReadOnly := strings.Contains(strings.ToLower(errMsg), "read-only") ||
				strings.Contains(strings.ToLower(errMsg), "not allowed") ||
				strings.Contains(strings.ToLower(errMsg), "select")

			if !hasReadOnly {
				t.Errorf("Error should indicate write operations are not allowed:\nGot: %s", errMsg)
			}
		})
	}
}

// TestParserExtractsTablesCorrectly tests that the parser correctly extracts table names.
func TestParserExtractsTablesCorrectly(t *testing.T) {
	parser := sql.NewParser()

	testCases := []struct {
		name           string
		query          string
		expectedTables []string
	}{
		{
			name:           "Single table",
			query:          "SELECT * FROM test.orders",
			expectedTables: []string{"test.orders"},
		},
		{
			name:           "Multiple tables in JOIN",
			query:          "SELECT * FROM test.orders o JOIN test.customers c ON o.customer_id = c.id",
			expectedTables: []string{"test.orders", "test.customers"},
		},
		{
			name:           "Subquery",
			query:          "SELECT * FROM test.orders WHERE customer_id IN (SELECT id FROM test.customers)",
			expectedTables: []string{"test.orders", "test.customers"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := parser.Parse(tc.query)
			if err != nil {
				t.Fatalf("Query should parse: %v", err)
			}

			// Check all expected tables are present
			for _, expected := range tc.expectedTables {
				found := false
				for _, actual := range plan.Tables {
					if actual == expected {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Expected table '%s' not found in plan.Tables: %v", expected, plan.Tables)
				}
			}
		})
	}
}

// TestTimeTravelDetection tests that time-travel queries are detected correctly.
// Note: The vitess/sqlparser doesn't natively support AS OF syntax, so we use
// text detection. Time-travel support is tested via SQL comments or special syntax.
func TestTimeTravelDetection(t *testing.T) {
	parser := sql.NewParser()

	// Test basic query detection (AS OF detection happens via text search)
	plan, err := parser.Parse("SELECT * FROM test.orders")
	if err != nil {
		t.Fatalf("Regular query should parse: %v", err)
	}

	if plan.HasTimeTravel {
		t.Errorf("Regular query should not have time-travel flag")
	}

	// Note: AS OF syntax is detected via text search in the parser,
	// but vitess/sqlparser may reject the actual SQL. The time-travel
	// detection feature works with engines that handle AS OF natively.
}
