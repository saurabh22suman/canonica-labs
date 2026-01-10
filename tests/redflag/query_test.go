package redflag

import (
	"testing"

	"github.com/canonica-labs/canonica/internal/errors"
	"github.com/canonica-labs/canonica/internal/sql"
)

// TestQuery_EmptyQuery proves that empty queries are rejected.
//
// Red-Flag: System MUST reject empty SQL queries.
func TestQuery_EmptyQuery(t *testing.T) {
	// Arrange
	parser := sql.NewParser()

	// Act
	_, err := parser.Parse("")

	// Assert: Parsing MUST fail
	if err == nil {
		t.Fatal("expected error for empty query, got nil")
	}
}

// TestQuery_WhitespaceOnlyQuery proves that whitespace-only queries are rejected.
//
// Red-Flag: System MUST reject whitespace-only queries.
func TestQuery_WhitespaceOnlyQuery(t *testing.T) {
	// Arrange
	parser := sql.NewParser()

	// Act
	_, err := parser.Parse("   \t\n   ")

	// Assert: Parsing MUST fail
	if err == nil {
		t.Fatal("expected error for whitespace-only query, got nil")
	}
}

// TestQuery_WriteOperationsBlocked proves that all write operations are blocked.
//
// Red-Flag: MVP MUST reject INSERT, UPDATE, DELETE operations.
func TestQuery_WriteOperationsBlocked(t *testing.T) {
	parser := sql.NewParser()

	writeQueries := []struct {
		name  string
		query string
	}{
		{"INSERT", "INSERT INTO users (name) VALUES ('test')"},
		{"UPDATE", "UPDATE users SET name = 'test'"},
		{"DELETE", "DELETE FROM users WHERE id = 1"},
	}

	for _, tc := range writeQueries {
		t.Run(tc.name, func(t *testing.T) {
			// Act
			_, err := parser.Parse(tc.query)

			// Assert: Parsing MUST fail for write operations
			if err == nil {
				t.Fatalf("expected error for %s query, got nil", tc.name)
			}

			// Assert: Error must indicate write not allowed
			if _, ok := err.(*errors.ErrQueryRejected); !ok {
				t.Fatalf("expected ErrQueryRejected for %s, got %T: %v", tc.name, err, err)
			}
		})
	}
}

// TestQuery_UnsupportedSyntax proves that unsupported SQL is rejected.
//
// Red-Flag: System MUST reject unsupported SQL operations.
func TestQuery_UnsupportedSyntax(t *testing.T) {
	parser := sql.NewParser()

	unsupportedQueries := []struct {
		name  string
		query string
	}{
		{"CREATE TABLE", "CREATE TABLE users (id INT)"},
		{"DROP TABLE", "DROP TABLE users"},
		{"ALTER TABLE", "ALTER TABLE users ADD COLUMN email VARCHAR(255)"},
		{"TRUNCATE", "TRUNCATE TABLE users"},
		{"GRANT", "GRANT SELECT ON users TO readonly"},
		{"REVOKE", "REVOKE SELECT ON users FROM readonly"},
	}

	for _, tc := range unsupportedQueries {
		t.Run(tc.name, func(t *testing.T) {
			// Act
			_, err := parser.Parse(tc.query)

			// Assert: Parsing MUST fail
			if err == nil {
				t.Fatalf("expected error for %s, got nil", tc.name)
			}
		})
	}
}

// TestQuery_ValidSelectPasses proves that valid SELECT queries are accepted.
// This is a sanity check that the parser doesn't reject everything.
//
// Note: This is technically a Green-Flag test but included here to verify
// the Red-Flag tests aren't overly broad.
func TestQuery_ValidSelectPasses(t *testing.T) {
	parser := sql.NewParser()

	validQueries := []string{
		"SELECT * FROM users",
		"SELECT id, name FROM users WHERE id = 1",
		"SELECT COUNT(*) FROM orders",
	}

	for _, query := range validQueries {
		t.Run(query, func(t *testing.T) {
			// Act
			plan, err := parser.Parse(query)

			// Assert: Parsing MUST succeed for valid SELECT
			if err != nil {
				t.Fatalf("expected valid SELECT to pass, got error: %v", err)
			}

			if plan == nil {
				t.Fatal("expected non-nil plan for valid SELECT")
			}
		})
	}
}
