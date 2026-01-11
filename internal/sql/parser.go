// Package sql provides SQL parsing utilities for the canonica gateway.
// Uses dolthub/vitess sqlparser (enhanced fork) for SQL parsing with support for:
// - CTEs (WITH clause) - T013
// - Time-travel queries (AS OF) - T014
//
// Per docs/plan.md: MVP supports read-only SELECT queries only.
package sql

import (
	"strings"

	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/errors"
	"github.com/dolthub/vitess/go/vt/sqlparser"
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

	// TimeTravelTimestamp is the global AS OF timestamp if HasTimeTravel is true.
	// Deprecated: Use TimeTravelPerTable for per-table timestamps.
	TimeTravelTimestamp string

	// TimeTravelPerTable maps table names to their AS OF timestamps.
	// Per tracker.md T015: Enables per-table snapshot consistency validation.
	TimeTravelPerTable map[string]string
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
	var hasTimeTravel bool
	var timestamp string
	var perTableTimestamps map[string]string

	switch s := stmt.(type) {
	case *sqlparser.Select:
		op = capabilities.OperationSelect
		tables, hasTimeTravel, timestamp, perTableTimestamps = extractTablesFromSelectWithAsOf(s)

	case *sqlparser.SetOp:
		op = capabilities.OperationSelect
		tables, hasTimeTravel, timestamp, perTableTimestamps = extractTablesFromUnionWithAsOf(s)

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

	// Fallback: Also check for time-travel syntax via text search for edge cases
	// where AST parsing might not capture all temporal syntax variations
	if !hasTimeTravel {
		hasTimeTravel, timestamp = detectTimeTravel(sql)
	}

	return &LogicalPlan{
		RawSQL:              sql,
		Operation:           op,
		Tables:              tables,
		HasTimeTravel:       hasTimeTravel,
		TimeTravelTimestamp: timestamp,
		TimeTravelPerTable:  perTableTimestamps,
	}, nil
}

// extractTablesFromSelectWithAsOf extracts tables and AS OF from a SELECT statement.
// This is the enhanced version that returns time-travel information from AST.
// Also extracts tables from CTEs (WITH clause).
// Returns per-table timestamps for T015 snapshot consistency validation.
func extractTablesFromSelectWithAsOf(sel *sqlparser.Select) (tables []string, hasTimeTravel bool, timestamp string, perTable map[string]string) {
	seen := make(map[string]bool)
	cteNames := make(map[string]bool) // Track CTE names to exclude from final table list
	perTable = make(map[string]string)

	// Extract tables from CTEs (WITH clause) first
	if sel.With != nil {
		for _, cte := range sel.With.Ctes {
			// Record CTE name to exclude later (it's not a real table)
			if cte.As.String() != "" {
				cteNames[cte.As.String()] = true
			}
			// Extract underlying tables from CTE definition
			if cte.Expr != nil {
				if subquery, ok := cte.Expr.(*sqlparser.Subquery); ok {
					extractTablesFromSelectStatementWithAsOf(subquery.Select, &tables, seen, &hasTimeTravel, &timestamp, perTable)
				}
			}
		}
	}

	// Extract from FROM clause
	for _, tableExpr := range sel.From {
		extractTablesFromTableExprWithAsOf(tableExpr, &tables, seen, &hasTimeTravel, &timestamp, perTable)
	}

	// Extract from WHERE clause (subqueries)
	if sel.Where != nil {
		extractTablesFromExprWithAsOf(sel.Where.Expr, &tables, seen, &hasTimeTravel, &timestamp, perTable)
	}

	// Extract from HAVING clause (subqueries)
	if sel.Having != nil {
		extractTablesFromExprWithAsOf(sel.Having.Expr, &tables, seen, &hasTimeTravel, &timestamp, perTable)
	}

	// Extract from SELECT expressions (subqueries)
	for _, expr := range sel.SelectExprs {
		if aliased, ok := expr.(*sqlparser.AliasedExpr); ok {
			extractTablesFromExprWithAsOf(aliased.Expr, &tables, seen, &hasTimeTravel, &timestamp, perTable)
		}
	}

	// Filter out CTE names from the table list (they're not real tables)
	filteredTables := make([]string, 0, len(tables))
	for _, t := range tables {
		if !cteNames[t] {
			filteredTables = append(filteredTables, t)
		}
	}

	return filteredTables, hasTimeTravel, timestamp, perTable
}

// extractTablesFromUnionWithAsOf extracts tables and AS OF from a UNION statement.
func extractTablesFromUnionWithAsOf(union *sqlparser.SetOp) (tables []string, hasTimeTravel bool, timestamp string, perTable map[string]string) {
	seen := make(map[string]bool)
	perTable = make(map[string]string)

	// Extract from left side
	extractTablesFromSelectStatementWithAsOf(union.Left, &tables, seen, &hasTimeTravel, &timestamp, perTable)

	// Extract from right side
	extractTablesFromSelectStatementWithAsOf(union.Right, &tables, seen, &hasTimeTravel, &timestamp, perTable)

	return tables, hasTimeTravel, timestamp, perTable
}

// extractTablesFromSelectStatementWithAsOf extracts tables from any SelectStatement with AS OF tracking.
func extractTablesFromSelectStatementWithAsOf(stmt sqlparser.SelectStatement, tables *[]string, seen map[string]bool, hasTimeTravel *bool, timestamp *string, perTable map[string]string) {
	switch s := stmt.(type) {
	case *sqlparser.Select:
		// Handle CTEs
		if s.With != nil {
			for _, cte := range s.With.Ctes {
				if cte.Expr != nil {
					if subquery, ok := cte.Expr.(*sqlparser.Subquery); ok {
						extractTablesFromSelectStatementWithAsOf(subquery.Select, tables, seen, hasTimeTravel, timestamp, perTable)
					}
				}
			}
		}
		for _, tableExpr := range s.From {
			extractTablesFromTableExprWithAsOf(tableExpr, tables, seen, hasTimeTravel, timestamp, perTable)
		}
		if s.Where != nil {
			extractTablesFromExprWithAsOf(s.Where.Expr, tables, seen, hasTimeTravel, timestamp, perTable)
		}
	case *sqlparser.SetOp:
		extractTablesFromSelectStatementWithAsOf(s.Left, tables, seen, hasTimeTravel, timestamp, perTable)
		extractTablesFromSelectStatementWithAsOf(s.Right, tables, seen, hasTimeTravel, timestamp, perTable)
	case *sqlparser.ParenSelect:
		extractTablesFromSelectStatementWithAsOf(s.Select, tables, seen, hasTimeTravel, timestamp, perTable)
	}
}

// extractTablesFromTableExprWithAsOf extracts table names and AS OF from a table expression.
func extractTablesFromTableExprWithAsOf(expr sqlparser.TableExpr, tables *[]string, seen map[string]bool, hasTimeTravel *bool, timestamp *string, perTable map[string]string) {
	switch t := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		// Extract table name first so we can record per-table timestamp
		var tableName string
		switch e := t.Expr.(type) {
		case sqlparser.TableName:
			tableName = formatTableName(e)
			if tableName != "" && !seen[tableName] {
				*tables = append(*tables, tableName)
				seen[tableName] = true
			}
		case *sqlparser.Subquery:
			extractTablesFromSelectStatementWithAsOf(e.Select, tables, seen, hasTimeTravel, timestamp, perTable)
		}
		// Check for AS OF on this table - record per-table timestamp
		if t.AsOf != nil && t.AsOf.Time != nil {
			*hasTimeTravel = true
			ts := sqlparser.String(t.AsOf.Time)
			*timestamp = ts
			// T015: Record per-table timestamp for snapshot consistency validation
			if tableName != "" && perTable != nil {
				perTable[tableName] = ts
			}
		}
	case *sqlparser.JoinTableExpr:
		extractTablesFromTableExprWithAsOf(t.LeftExpr, tables, seen, hasTimeTravel, timestamp, perTable)
		extractTablesFromTableExprWithAsOf(t.RightExpr, tables, seen, hasTimeTravel, timestamp, perTable)
	case *sqlparser.ParenTableExpr:
		for _, tableExpr := range t.Exprs {
			extractTablesFromTableExprWithAsOf(tableExpr, tables, seen, hasTimeTravel, timestamp, perTable)
		}
	}
}

// extractTablesFromExprWithAsOf extracts tables from expressions (subqueries) with AS OF tracking.
func extractTablesFromExprWithAsOf(expr sqlparser.Expr, tables *[]string, seen map[string]bool, hasTimeTravel *bool, timestamp *string, perTable map[string]string) {
	if expr == nil {
		return
	}

	switch e := expr.(type) {
	case *sqlparser.Subquery:
		extractTablesFromSelectStatementWithAsOf(e.Select, tables, seen, hasTimeTravel, timestamp, perTable)
	case *sqlparser.AndExpr:
		extractTablesFromExprWithAsOf(e.Left, tables, seen, hasTimeTravel, timestamp, perTable)
		extractTablesFromExprWithAsOf(e.Right, tables, seen, hasTimeTravel, timestamp, perTable)
	case *sqlparser.OrExpr:
		extractTablesFromExprWithAsOf(e.Left, tables, seen, hasTimeTravel, timestamp, perTable)
		extractTablesFromExprWithAsOf(e.Right, tables, seen, hasTimeTravel, timestamp, perTable)
	case *sqlparser.ComparisonExpr:
		extractTablesFromExprWithAsOf(e.Left, tables, seen, hasTimeTravel, timestamp, perTable)
		extractTablesFromExprWithAsOf(e.Right, tables, seen, hasTimeTravel, timestamp, perTable)
	case *sqlparser.ParenExpr:
		extractTablesFromExprWithAsOf(e.Expr, tables, seen, hasTimeTravel, timestamp, perTable)
	case *sqlparser.RangeCond:
		extractTablesFromExprWithAsOf(e.Left, tables, seen, hasTimeTravel, timestamp, perTable)
		extractTablesFromExprWithAsOf(e.From, tables, seen, hasTimeTravel, timestamp, perTable)
		extractTablesFromExprWithAsOf(e.To, tables, seen, hasTimeTravel, timestamp, perTable)
	case *sqlparser.IsExpr:
		extractTablesFromExprWithAsOf(e.Expr, tables, seen, hasTimeTravel, timestamp, perTable)
	case *sqlparser.NotExpr:
		extractTablesFromExprWithAsOf(e.Expr, tables, seen, hasTimeTravel, timestamp, perTable)
	case *sqlparser.ExistsExpr:
		extractTablesFromSelectStatementWithAsOf(e.Subquery.Select, tables, seen, hasTimeTravel, timestamp, perTable)
	case *sqlparser.FuncExpr:
		for _, arg := range e.Exprs {
			if aliased, ok := arg.(*sqlparser.AliasedExpr); ok {
				extractTablesFromExprWithAsOf(aliased.Expr, tables, seen, hasTimeTravel, timestamp, perTable)
			}
		}
	case *sqlparser.CaseExpr:
		extractTablesFromExprWithAsOf(e.Expr, tables, seen, hasTimeTravel, timestamp, perTable)
		for _, when := range e.Whens {
			extractTablesFromExprWithAsOf(when.Cond, tables, seen, hasTimeTravel, timestamp, perTable)
			extractTablesFromExprWithAsOf(when.Val, tables, seen, hasTimeTravel, timestamp, perTable)
		}
		extractTablesFromExprWithAsOf(e.Else, tables, seen, hasTimeTravel, timestamp, perTable)
	}
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
	cteNames := make(map[string]bool)

	// Extract tables from CTEs (WITH clause) first
	if sel.With != nil {
		for _, cte := range sel.With.Ctes {
			// Record CTE name to exclude later
			if cte.As.String() != "" {
				cteNames[cte.As.String()] = true
			}
			// Extract underlying tables from CTE definition
			if cte.Expr != nil {
				if subquery, ok := cte.Expr.(*sqlparser.Subquery); ok {
					extractTablesFromSelectStatement(subquery.Select, &tables, seen)
				}
			}
		}
	}

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

	// Filter out CTE names
	filteredTables := make([]string, 0, len(tables))
	for _, t := range tables {
		if !cteNames[t] {
			filteredTables = append(filteredTables, t)
		}
	}

	return filteredTables
}

// extractTablesFromUnion extracts tables from a UNION statement.
func extractTablesFromUnion(union *sqlparser.SetOp) []string {
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
		// Handle CTEs
		if s.With != nil {
			for _, cte := range s.With.Ctes {
				if cte.Expr != nil {
					if subquery, ok := cte.Expr.(*sqlparser.Subquery); ok {
						extractTablesFromSelectStatement(subquery.Select, tables, seen)
					}
				}
			}
		}
		for _, tableExpr := range s.From {
			extractTablesFromTableExpr(tableExpr, tables, seen)
		}
		if s.Where != nil {
			extractTablesFromExpr(s.Where.Expr, tables, seen)
		}
	case *sqlparser.SetOp:
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
// Handles schema-qualified names (schema.table) and database-qualified names (db.schema.table).
// dolthub/vitess uses DbQualifier for database and SchemaQualifier for schema.
func formatTableName(tn sqlparser.TableName) string {
	name := tn.Name.String()
	
	// Build qualified name: [db.][schema.]table
	if !tn.SchemaQualifier.IsEmpty() {
		name = tn.SchemaQualifier.String() + "." + name
	}
	if !tn.DbQualifier.IsEmpty() {
		name = tn.DbQualifier.String() + "." + name
	}
	
	return name
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
//
// NOTE: CTEs (WITH clause) are now supported via dolthub/vitess parser (T013).
func detectUnsupportedSyntax(sql string) error {
	upperSQL := strings.ToUpper(sql)

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
//
// NOTE: CTEs (WITH clause) are now supported via dolthub/vitess parser (T013).
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

	return nil
}
