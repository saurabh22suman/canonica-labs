// Package sql provides SQL parsing utilities for the canonica gateway.
// Uses vitess/sqlparser to parse SQL and extract relevant information.
//
// Per docs/plan.md: MVP supports read-only SELECT queries only.
package sql

import (
	"strings"

	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/errors"
)

// LogicalPlan represents a parsed SQL query's logical structure.
type LogicalPlan struct {
	// RawSQL is the original SQL query.
	RawSQL string

	// Operation is the type of SQL operation (SELECT, INSERT, etc.).
	Operation capabilities.OperationType

	// Tables are the table names referenced in the query.
	Tables []string

	// HasTimeTravel indicates if the query uses time-travel (AS OF).
	HasTimeTravel bool

	// TimeTravelTimestamp is the AS OF timestamp if HasTimeTravel is true.
	TimeTravelTimestamp string
}

// Parser parses SQL queries into logical plans.
type Parser struct{}

// NewParser creates a new SQL parser.
func NewParser() *Parser {
	return &Parser{}
}

// Parse parses a SQL query into a LogicalPlan.
// Returns an error if the query is invalid or uses unsupported syntax.
func (p *Parser) Parse(sql string) (*LogicalPlan, error) {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return nil, errors.NewQueryRejected(sql, "empty query", "provide a valid SQL query")
	}

	// Determine operation type from first keyword
	upperSQL := strings.ToUpper(sql)
	var op capabilities.OperationType

	switch {
	case strings.HasPrefix(upperSQL, "SELECT"):
		op = capabilities.OperationSelect
	case strings.HasPrefix(upperSQL, "INSERT"):
		op = capabilities.OperationInsert
	case strings.HasPrefix(upperSQL, "UPDATE"):
		op = capabilities.OperationUpdate
	case strings.HasPrefix(upperSQL, "DELETE"):
		op = capabilities.OperationDelete
	default:
		return nil, errors.NewQueryRejected(sql,
			"unsupported SQL operation",
			"only SELECT queries are supported in MVP")
	}

	// Block write operations in MVP
	if op.IsWriteOperation() {
		return nil, errors.NewWriteNotAllowed(string(op))
	}

	// Extract table references
	// NOTE: This is a simplified implementation. Full implementation should use
	// vitess/sqlparser for proper AST parsing. See below for integration notes.
	tables := extractTableReferences(sql)

	// Check for time-travel syntax (AS OF)
	hasTimeTravel, timestamp := detectTimeTravel(sql)

	return &LogicalPlan{
		RawSQL:              sql,
		Operation:           op,
		Tables:              tables,
		HasTimeTravel:       hasTimeTravel,
		TimeTravelTimestamp: timestamp,
	}, nil
}

// extractTableReferences extracts table names from a SQL query.
// This is a simplified implementation for MVP.
// TODO: Replace with vitess/sqlparser AST traversal for production.
func extractTableReferences(sql string) []string {
	// Simplified extraction: look for FROM and JOIN clauses
	// This is NOT production-ready - just scaffolding for the interface
	tables := []string{}

	upperSQL := strings.ToUpper(sql)

	// Find FROM clause
	fromIdx := strings.Index(upperSQL, "FROM")
	if fromIdx == -1 {
		return tables
	}

	// Extract text after FROM until next keyword
	afterFrom := sql[fromIdx+4:]
	afterFrom = strings.TrimSpace(afterFrom)

	// Find end of table reference (next keyword or end)
	keywords := []string{"WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "GROUP", "ORDER", "LIMIT", "HAVING", ";"}
	endIdx := len(afterFrom)
	for _, kw := range keywords {
		if idx := strings.Index(strings.ToUpper(afterFrom), kw); idx != -1 && idx < endIdx {
			endIdx = idx
		}
	}

	tablePart := strings.TrimSpace(afterFrom[:endIdx])
	if tablePart != "" {
		// Handle comma-separated tables
		parts := strings.Split(tablePart, ",")
		for _, part := range parts {
			part = strings.TrimSpace(part)
			// Remove alias (e.g., "table AS t" -> "table")
			if spaceIdx := strings.Index(part, " "); spaceIdx != -1 {
				part = part[:spaceIdx]
			}
			if part != "" {
				tables = append(tables, part)
			}
		}
	}

	return tables
}

// detectTimeTravel checks for AS OF syntax in the query.
// Returns true and the timestamp if found.
func detectTimeTravel(sql string) (bool, string) {
	upperSQL := strings.ToUpper(sql)
	asOfIdx := strings.Index(upperSQL, "AS OF")
	if asOfIdx == -1 {
		return false, ""
	}

	// Extract timestamp after AS OF
	afterAsOf := sql[asOfIdx+5:]
	afterAsOf = strings.TrimSpace(afterAsOf)

	// Find end of timestamp (next keyword or end)
	keywords := []string{"WHERE", "GROUP", "ORDER", "LIMIT", "HAVING", ";"}
	endIdx := len(afterAsOf)
	for _, kw := range keywords {
		if idx := strings.Index(strings.ToUpper(afterAsOf), kw); idx != -1 && idx < endIdx {
			endIdx = idx
		}
	}

	timestamp := strings.TrimSpace(afterAsOf[:endIdx])
	return true, timestamp
}

// ValidateQuery validates a SQL query without executing it.
// Returns the logical plan if valid, or an error if not.
func (p *Parser) ValidateQuery(sql string) (*LogicalPlan, error) {
	return p.Parse(sql)
}
