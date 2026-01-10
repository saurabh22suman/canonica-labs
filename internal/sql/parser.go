// Package sql provides SQL parsing utilities for the canonica gateway.
// Uses vitess/sqlparser to parse SQL and extract relevant information.
//
// Per docs/plan.md: MVP supports read-only SELECT queries only.
package sql

import (
	"strings"

	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/errors"
	"github.com/xwb1989/sqlparser"
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
// Per phase-3-spec.md §9: "Parser rejections must be explicit, stable, and human-readable."
func (p *Parser) Parse(sql string) (*LogicalPlan, error) {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return nil, errors.NewQueryRejected(sql, "empty query", "provide a valid SQL query")
	}

	// Check for multiple statements (security: prevent SQL injection)
	stmts, err := sqlparser.SplitStatementToPieces(sql)
	if err != nil {
		return nil, errors.NewQueryRejected(sql, "failed to parse SQL", err.Error())
	}
	if len(stmts) > 1 {
		return nil, errors.NewQueryRejected(sql,
			"multiple statements not allowed",
			"submit one query at a time")
	}

	// Phase 3: Pre-parse detection of unsupported syntax constructs
	// Per phase-3-spec.md §9: Must detect and report these BEFORE generic parse errors
	if err := detectUnsupportedSyntax(sql); err != nil {
		return nil, err
	}

	// Check for vendor-specific hints
	if err := detectVendorHints(sql); err != nil {
		return nil, err
	}

	// Parse the SQL into an AST
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		// Phase 3: Attempt to classify the parse error more specifically
		if classifiedErr := classifyParseError(sql, err); classifiedErr != nil {
			return nil, classifiedErr
		}
		return nil, errors.NewQueryRejected(sql, "invalid SQL syntax", err.Error())
	}

	// Determine operation type and validate
	var op capabilities.OperationType
	var tables []string

	switch s := stmt.(type) {
	case *sqlparser.Select:
		op = capabilities.OperationSelect
		tables = extractTablesFromSelect(s)

	case *sqlparser.Union:
		op = capabilities.OperationSelect
		tables = extractTablesFromUnion(s)

	case *sqlparser.Insert:
		op = capabilities.OperationInsert
		return nil, errors.NewWriteNotAllowed(string(op))

	case *sqlparser.Update:
		op = capabilities.OperationUpdate
		return nil, errors.NewWriteNotAllowed(string(op))

	case *sqlparser.Delete:
		op = capabilities.OperationDelete
		return nil, errors.NewWriteNotAllowed(string(op))

	case *sqlparser.DDL:
		return nil, errors.NewQueryRejected(sql,
			"DDL statements not allowed",
			"only SELECT queries are supported")

	case *sqlparser.DBDDL:
		return nil, errors.NewQueryRejected(sql,
			"database DDL statements not allowed",
			"only SELECT queries are supported")

	case *sqlparser.Show:
		return nil, errors.NewQueryRejected(sql,
			"SHOW statements not allowed",
			"only SELECT queries are supported")

	case *sqlparser.Set:
		return nil, errors.NewQueryRejected(sql,
			"SET statements not allowed",
			"only SELECT queries are supported")

	default:
		return nil, errors.NewQueryRejected(sql,
			"unsupported SQL operation",
			"only SELECT queries are supported in MVP")
	}

	// Check for time-travel syntax (AS OF)
	// Note: vitess/sqlparser doesn't natively support AS OF, so we fallback to text search
	hasTimeTravel, timestamp := detectTimeTravel(sql)

	return &LogicalPlan{
		RawSQL:              sql,
		Operation:           op,
		Tables:              tables,
		HasTimeTravel:       hasTimeTravel,
		TimeTravelTimestamp: timestamp,
	}, nil
}

// extractTablesFromSelect extracts all table names from a SELECT statement.
// This includes tables from:
// - FROM clause (with aliases resolved)
// - JOINs
// - Subqueries in FROM, WHERE, SELECT
// - CTEs (WITH clause)
func extractTablesFromSelect(sel *sqlparser.Select) []string {
	tables := make([]string, 0)
	seen := make(map[string]bool)

	// Extract from FROM clause
	for _, tableExpr := range sel.From {
		extractTablesFromTableExpr(tableExpr, &tables, seen)
	}

	// Extract from WHERE clause (subqueries)
	if sel.Where != nil {
		extractTablesFromExpr(sel.Where.Expr, &tables, seen)
	}

	// Extract from HAVING clause (subqueries)
	if sel.Having != nil {
		extractTablesFromExpr(sel.Having.Expr, &tables, seen)
	}

	// Extract from SELECT expressions (subqueries)
	for _, expr := range sel.SelectExprs {
		if aliased, ok := expr.(*sqlparser.AliasedExpr); ok {
			extractTablesFromExpr(aliased.Expr, &tables, seen)
		}
	}

	return tables
}

// extractTablesFromUnion extracts tables from a UNION statement.
func extractTablesFromUnion(union *sqlparser.Union) []string {
	tables := make([]string, 0)
	seen := make(map[string]bool)

	// Extract from left side
	extractTablesFromSelectStatement(union.Left, &tables, seen)

	// Extract from right side
	extractTablesFromSelectStatement(union.Right, &tables, seen)

	return tables
}

// extractTablesFromSelectStatement extracts tables from any SelectStatement.
func extractTablesFromSelectStatement(stmt sqlparser.SelectStatement, tables *[]string, seen map[string]bool) {
	switch s := stmt.(type) {
	case *sqlparser.Select:
		for _, tableExpr := range s.From {
			extractTablesFromTableExpr(tableExpr, tables, seen)
		}
		if s.Where != nil {
			extractTablesFromExpr(s.Where.Expr, tables, seen)
		}
	case *sqlparser.Union:
		extractTablesFromSelectStatement(s.Left, tables, seen)
		extractTablesFromSelectStatement(s.Right, tables, seen)
	case *sqlparser.ParenSelect:
		extractTablesFromSelectStatement(s.Select, tables, seen)
	}
}

// extractTablesFromTableExpr extracts table names from a table expression.
func extractTablesFromTableExpr(expr sqlparser.TableExpr, tables *[]string, seen map[string]bool) {
	switch t := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		// This is a table reference (possibly with alias)
		switch e := t.Expr.(type) {
		case sqlparser.TableName:
			tableName := formatTableName(e)
			if tableName != "" && !seen[tableName] {
				*tables = append(*tables, tableName)
				seen[tableName] = true
			}
		case *sqlparser.Subquery:
			// Subquery in FROM clause
			extractTablesFromSelectStatement(e.Select, tables, seen)
		}
	case *sqlparser.JoinTableExpr:
		// JOIN expression - extract from both sides
		extractTablesFromTableExpr(t.LeftExpr, tables, seen)
		extractTablesFromTableExpr(t.RightExpr, tables, seen)
	case *sqlparser.ParenTableExpr:
		// Parenthesized table expression
		for _, tableExpr := range t.Exprs {
			extractTablesFromTableExpr(tableExpr, tables, seen)
		}
	}
}

// extractTablesFromExpr extracts tables from any expression (for subqueries).
func extractTablesFromExpr(expr sqlparser.Expr, tables *[]string, seen map[string]bool) {
	if expr == nil {
		return
	}

	switch e := expr.(type) {
	case *sqlparser.Subquery:
		extractTablesFromSelectStatement(e.Select, tables, seen)
	case *sqlparser.AndExpr:
		extractTablesFromExpr(e.Left, tables, seen)
		extractTablesFromExpr(e.Right, tables, seen)
	case *sqlparser.OrExpr:
		extractTablesFromExpr(e.Left, tables, seen)
		extractTablesFromExpr(e.Right, tables, seen)
	case *sqlparser.ComparisonExpr:
		extractTablesFromExpr(e.Left, tables, seen)
		extractTablesFromExpr(e.Right, tables, seen)
	case *sqlparser.ParenExpr:
		extractTablesFromExpr(e.Expr, tables, seen)
	case *sqlparser.RangeCond:
		extractTablesFromExpr(e.Left, tables, seen)
		extractTablesFromExpr(e.From, tables, seen)
		extractTablesFromExpr(e.To, tables, seen)
	case *sqlparser.IsExpr:
		extractTablesFromExpr(e.Expr, tables, seen)
	case *sqlparser.NotExpr:
		extractTablesFromExpr(e.Expr, tables, seen)
	case *sqlparser.ExistsExpr:
		extractTablesFromSelectStatement(e.Subquery.Select, tables, seen)
	case *sqlparser.FuncExpr:
		for _, arg := range e.Exprs {
			if aliased, ok := arg.(*sqlparser.AliasedExpr); ok {
				extractTablesFromExpr(aliased.Expr, tables, seen)
			}
		}
	case *sqlparser.CaseExpr:
		extractTablesFromExpr(e.Expr, tables, seen)
		for _, when := range e.Whens {
			extractTablesFromExpr(when.Cond, tables, seen)
			extractTablesFromExpr(when.Val, tables, seen)
		}
		extractTablesFromExpr(e.Else, tables, seen)
	}
}

// formatTableName formats a TableName into a string.
// Handles schema-qualified names (schema.table).
func formatTableName(tn sqlparser.TableName) string {
	if tn.Qualifier.IsEmpty() {
		return tn.Name.String()
	}
	return tn.Qualifier.String() + "." + tn.Name.String()
}

// detectTimeTravel checks for AS OF syntax in the query.
// Returns true and the timestamp if found.
// Note: This uses text search as vitess/sqlparser doesn't natively support AS OF.
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

// ValidateTableName validates that a table name is fully qualified.
// Per phase-2-spec.md §6: Schema-qualified table names are required.
// Format: <schema>.<table>
//
// Returns an error if the name is not fully qualified.
func ValidateTableName(name string) error {
	if name == "" {
		return errors.NewInvalidTableDefinition("name", "table name cannot be empty")
	}

	// Check for schema.table format
	parts := strings.Split(name, ".")
	if len(parts) != 2 {
		return errors.NewInvalidTableDefinition("name",
			"fully-qualified name required: <schema>.<table>. Got: '"+name+"'")
	}

	schema := parts[0]
	table := parts[1]

	// Both schema and table must be non-empty
	if schema == "" {
		return errors.NewInvalidTableDefinition("name",
			"schema cannot be empty. Required format: <schema>.<table>")
	}

	if table == "" {
		return errors.NewInvalidTableDefinition("name",
			"table cannot be empty. Required format: <schema>.<table>")
	}

	return nil
}

// IsQualifiedTableName checks if a table name is fully qualified (schema.table).
func IsQualifiedTableName(name string) bool {
	return ValidateTableName(name) == nil
}

// detectUnsupportedSyntax performs pre-parse detection of unsupported SQL constructs.
// Per phase-3-spec.md §9: These must be detected BEFORE generic parse errors.
// Returns an error if unsupported syntax is detected, nil otherwise.
func detectUnsupportedSyntax(sql string) error {
	upperSQL := strings.ToUpper(sql)

	// Check for CTEs (WITH clause at the beginning)
	// Per phase-3-spec.md §9: CTEs must fail with specific error
	if strings.HasPrefix(strings.TrimSpace(upperSQL), "WITH ") ||
		strings.HasPrefix(strings.TrimSpace(upperSQL), "WITH\n") ||
		strings.HasPrefix(strings.TrimSpace(upperSQL), "WITH\t") {
		return errors.NewUnsupportedSyntax(
			"CTE (WITH clause)",
			"simple SELECT without WITH clauses; use subqueries instead",
		)
	}

	// Check for WINDOW functions (OVER clause)
	// Per phase-3-spec.md §9: WINDOW functions must fail with specific error
	if containsWindowFunction(upperSQL) {
		return errors.NewUnsupportedSyntax(
			"WINDOW FUNCTION (OVER clause)",
			"simple SELECT with GROUP BY for aggregation",
		)
	}

	return nil
}

// containsWindowFunction checks if the SQL contains window function syntax.
// Window functions are identified by the OVER keyword following a function call.
func containsWindowFunction(upperSQL string) bool {
	// List of common window functions
	windowFuncs := []string{
		"ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE",
		"LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE",
		"CUME_DIST", "PERCENT_RANK",
	}

	// Check for common window functions followed by OVER
	for _, fn := range windowFuncs {
		if strings.Contains(upperSQL, fn+"(") && strings.Contains(upperSQL, " OVER") {
			return true
		}
		if strings.Contains(upperSQL, fn+" (") && strings.Contains(upperSQL, " OVER") {
			return true
		}
	}

	// Check for aggregate functions with OVER (they become window functions)
	aggregates := []string{"SUM", "COUNT", "AVG", "MIN", "MAX"}
	for _, agg := range aggregates {
		// Look for pattern: AGG(...) OVER
		idx := strings.Index(upperSQL, agg+"(")
		if idx == -1 {
			idx = strings.Index(upperSQL, agg+" (")
		}
		if idx != -1 {
			// Find matching closing paren and check for OVER
			afterAgg := upperSQL[idx:]
			parenCount := 0
			inParen := false
			for i, c := range afterAgg {
				if c == '(' {
					parenCount++
					inParen = true
				} else if c == ')' {
					parenCount--
					if inParen && parenCount == 0 {
						// Check what comes after the closing paren
						remaining := strings.TrimSpace(afterAgg[i+1:])
						if strings.HasPrefix(remaining, "OVER") {
							return true
						}
						break
					}
				}
			}
		}
	}

	return false
}

// detectVendorHints checks for vendor-specific SQL hints.
// Per phase-3-spec.md §9: Vendor-specific hints must fail with specific error.
func detectVendorHints(sql string) error {
	upperSQL := strings.ToUpper(sql)

	// MySQL-style index hints
	if strings.Contains(upperSQL, " USE INDEX") ||
		strings.Contains(upperSQL, " USE KEY") {
		return errors.NewVendorHint("USE INDEX")
	}

	if strings.Contains(upperSQL, " FORCE INDEX") ||
		strings.Contains(upperSQL, " FORCE KEY") {
		return errors.NewVendorHint("FORCE INDEX")
	}

	if strings.Contains(upperSQL, " IGNORE INDEX") ||
		strings.Contains(upperSQL, " IGNORE KEY") {
		return errors.NewVendorHint("IGNORE INDEX")
	}

	// Oracle-style optimizer hints (/*+ ... */)
	if strings.Contains(sql, "/*+") {
		return errors.NewVendorHint("OPTIMIZER HINT (/*+ ... */)")
	}

	return nil
}

// classifyParseError attempts to classify a parse error more specifically.
// Per phase-3-spec.md §9: Generic parse errors should be avoided where classification is possible.
func classifyParseError(sql string, parseErr error) error {
	errStr := parseErr.Error()
	upperSQL := strings.ToUpper(sql)

	// Check if the error is near an OVER keyword (window function)
	if strings.Contains(errStr, "OVER") ||
		(strings.Contains(upperSQL, " OVER") && strings.Contains(upperSQL, "(")) {
		return errors.NewUnsupportedSyntax(
			"WINDOW FUNCTION (OVER clause)",
			"simple SELECT with GROUP BY for aggregation",
		)
	}

	// Check if error is related to WITH (CTE)
	if strings.Contains(errStr, "WITH") ||
		strings.HasPrefix(strings.TrimSpace(upperSQL), "WITH") {
		return errors.NewUnsupportedSyntax(
			"CTE (WITH clause)",
			"simple SELECT without WITH clauses; use subqueries instead",
		)
	}

	// Check for RECURSIVE keyword
	if strings.Contains(upperSQL, "RECURSIVE") {
		return errors.NewUnsupportedSyntax(
			"RECURSIVE CTE",
			"simple SELECT without recursive queries",
		)
	}

	return nil
}
