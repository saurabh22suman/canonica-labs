// Package federation provides cross-engine query federation.
//
// Per phase-9-spec.md: "Execute queries that span multiple engines,
// enabling JOINs between Iceberg tables on Trino, Delta tables on Spark,
// and warehouse tables on Snowflake through a single SQL query."
package federation

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/canonica-labs/canonica/internal/catalog"
	"github.com/canonica-labs/canonica/internal/sql"
	"github.com/canonica-labs/canonica/internal/storage"
)

// JoinType represents the type of SQL join.
type JoinType string

const (
	JoinTypeInner JoinType = "INNER"
	JoinTypeLeft  JoinType = "LEFT"
	JoinTypeRight JoinType = "RIGHT"
	JoinTypeFull  JoinType = "FULL"
	JoinTypeCross JoinType = "CROSS"
)

// QueryAnalysis contains the result of analyzing a SQL query.
// Per phase-9-spec.md §1.1.
type QueryAnalysis struct {
	// OriginalSQL is the original query text.
	OriginalSQL string

	// TablesByEngine groups tables by their execution engine.
	TablesByEngine map[string][]*TableRef

	// IsCrossEngine indicates if this query spans multiple engines.
	IsCrossEngine bool

	// Joins contains the join conditions extracted from the query.
	Joins []*JoinCondition

	// PushablePredicates are predicates that can be pushed to each engine.
	// Keyed by table full name.
	PushablePredicates map[string][]*Predicate

	// RequiredColumns are columns needed from each table.
	// Keyed by table full name.
	RequiredColumns map[string][]string

	// Aggregations are aggregate functions (must be done post-join).
	Aggregations []*Aggregation

	// OrderBy clauses (must be done post-join if cross-engine).
	OrderBy []*OrderByClause

	// Limit value (applied after join).
	Limit *int
}

// TableRef represents a table reference in a query.
type TableRef struct {
	// Schema is the schema/database name.
	Schema string

	// Name is the table name.
	Name string

	// Alias is the table alias (if any).
	Alias string

	// Engine is the execution engine for this table.
	Engine string

	// Format is the table format (Iceberg, Delta, etc.).
	Format catalog.TableFormat
}

// FullName returns the fully qualified table name.
func (t *TableRef) FullName() string {
	if t.Schema != "" {
		return t.Schema + "." + t.Name
	}
	return t.Name
}

// DisplayName returns the alias if set, otherwise the full name.
func (t *TableRef) DisplayName() string {
	if t.Alias != "" {
		return t.Alias
	}
	return t.FullName()
}

// JoinCondition represents a join condition between tables.
type JoinCondition struct {
	// Type is the join type (INNER, LEFT, etc.).
	Type JoinType

	// LeftTable is the left side table (alias or name).
	LeftTable string

	// LeftCol is the left side column.
	LeftCol string

	// RightTable is the right side table (alias or name).
	RightTable string

	// RightCol is the right side column.
	RightCol string

	// Operator is the join operator (=, <, >, etc.).
	Operator string
}

// Predicate represents a WHERE clause predicate.
type Predicate struct {
	// Table is the table this predicate applies to.
	Table string

	// Column is the column name.
	Column string

	// Operator is the comparison operator.
	Operator string

	// Value is the literal value being compared.
	Value interface{}

	// Raw is the original SQL fragment.
	Raw string
}

// Aggregation represents an aggregate function.
type Aggregation struct {
	// Function is the aggregate function (SUM, COUNT, etc.).
	Function string

	// Column is the column being aggregated.
	Column string

	// Alias is the result alias.
	Alias string

	// Raw is the original SQL fragment.
	Raw string
}

// OrderByClause represents an ORDER BY clause.
type OrderByClause struct {
	// Column is the column to order by.
	Column string

	// Descending indicates DESC order.
	Descending bool
}

// Analyzer analyzes SQL queries for cross-engine federation.
type Analyzer struct {
	parser   *sql.Parser
	metadata storage.TableRepository
}

// NewAnalyzer creates a new query analyzer.
func NewAnalyzer(parser *sql.Parser, metadata storage.TableRepository) *Analyzer {
	return &Analyzer{
		parser:   parser,
		metadata: metadata,
	}
}

// Analyze parses a SQL query and determines if it's a cross-engine query.
// Per phase-9-spec.md §1.2.
func (a *Analyzer) Analyze(ctx context.Context, sqlQuery string) (*QueryAnalysis, error) {
	if sqlQuery == "" {
		return nil, fmt.Errorf("federation: empty query")
	}

	analysis := &QueryAnalysis{
		OriginalSQL:        sqlQuery,
		TablesByEngine:     make(map[string][]*TableRef),
		PushablePredicates: make(map[string][]*Predicate),
		RequiredColumns:    make(map[string][]string),
	}

	// Parse SQL to get logical plan
	logicalPlan, err := a.parser.Parse(sqlQuery)
	if err != nil {
		return nil, fmt.Errorf("federation: parse error: %w", err)
	}

	// Extract table references from the query
	tables, err := a.extractTables(ctx, logicalPlan)
	if err != nil {
		return nil, err
	}

	if len(tables) == 0 {
		return nil, fmt.Errorf("federation: no tables found in query")
	}

	// Look up each table's engine and format from metadata
	for _, table := range tables {
		vt, err := a.metadata.Get(ctx, table.FullName())
		if err != nil {
			return nil, fmt.Errorf("federation: table %s not found: %w", table.FullName(), err)
		}

		// Determine engine from table metadata
		if len(vt.Sources) > 0 && vt.Sources[0].Engine != "" {
			table.Engine = vt.Sources[0].Engine
		} else {
			// Default based on format
			table.Engine = a.defaultEngineForFormat(string(vt.Sources[0].Format))
		}

		if len(vt.Sources) > 0 {
			table.Format = catalog.TableFormat(string(vt.Sources[0].Format))
		}

		analysis.TablesByEngine[table.Engine] = append(
			analysis.TablesByEngine[table.Engine], table)
	}

	// Check if this is a cross-engine query
	analysis.IsCrossEngine = len(analysis.TablesByEngine) > 1

	if !analysis.IsCrossEngine {
		// Single engine - no decomposition needed
		return analysis, nil
	}

	// Extract join conditions
	analysis.Joins = a.extractJoins(sqlQuery, tables)

	// Extract pushable predicates
	analysis.PushablePredicates = a.extractPushablePredicates(sqlQuery, tables)

	// Extract required columns per table
	analysis.RequiredColumns = a.extractRequiredColumns(sqlQuery, tables, analysis.Joins)

	// Extract aggregations
	analysis.Aggregations = a.extractAggregations(sqlQuery)

	// Extract ORDER BY
	analysis.OrderBy = a.extractOrderBy(sqlQuery)

	// Extract LIMIT
	analysis.Limit = a.extractLimit(sqlQuery)

	return analysis, nil
}

// extractTables extracts table references from a logical plan.
func (a *Analyzer) extractTables(ctx context.Context, plan *sql.LogicalPlan) ([]*TableRef, error) {
	var tables []*TableRef

	for _, tableName := range plan.Tables {
		parts := strings.Split(tableName, ".")
		ref := &TableRef{}

		if len(parts) >= 2 {
			ref.Schema = parts[0]
			ref.Name = parts[1]
		} else {
			ref.Name = tableName
		}

		tables = append(tables, ref)
	}

	// Also extract aliases from the original SQL
	a.extractAliases(plan.RawSQL, tables)

	return tables, nil
}

// extractAliases extracts table aliases from raw SQL.
func (a *Analyzer) extractAliases(rawSQL string, tables []*TableRef) {
	// Pattern: table_name AS alias or table_name alias
	aliasPattern := regexp.MustCompile(`(?i)(\w+(?:\.\w+)*)\s+(?:AS\s+)?(\w+)\s*(?:ON|JOIN|WHERE|,|$)`)

	matches := aliasPattern.FindAllStringSubmatch(rawSQL, -1)
	for _, match := range matches {
		if len(match) >= 3 {
			tableName := match[1]
			alias := match[2]

			// Skip SQL keywords
			if isKeyword(alias) {
				continue
			}

			// Find the table and set its alias
			for _, table := range tables {
				if table.FullName() == tableName || table.Name == tableName {
					table.Alias = alias
					break
				}
			}
		}
	}
}

// extractJoins extracts join conditions from SQL.
func (a *Analyzer) extractJoins(sqlQuery string, tables []*TableRef) []*JoinCondition {
	var joins []*JoinCondition

	// Pattern: ON left.col = right.col
	joinPattern := regexp.MustCompile(
		`(?i)(?:(INNER|LEFT|RIGHT|FULL|CROSS)\s+)?JOIN\s+` +
			`\S+\s+(?:AS\s+)?(\w+)\s+ON\s+` +
			`(\w+)\.(\w+)\s*(=|<|>|<=|>=|<>)\s*(\w+)\.(\w+)`)

	matches := joinPattern.FindAllStringSubmatch(sqlQuery, -1)
	for _, match := range matches {
		if len(match) >= 8 {
			joinType := JoinTypeInner
			if match[1] != "" {
				joinType = JoinType(strings.ToUpper(match[1]))
			}

			joins = append(joins, &JoinCondition{
				Type:       joinType,
				LeftTable:  match[3],
				LeftCol:    match[4],
				Operator:   match[5],
				RightTable: match[6],
				RightCol:   match[7],
			})
		}
	}

	return joins
}

// extractPushablePredicates extracts predicates that can be pushed to each engine.
// Per phase-9-spec.md §1.3: Only single-table predicates can be pushed.
func (a *Analyzer) extractPushablePredicates(sqlQuery string, tables []*TableRef) map[string][]*Predicate {
	predicates := make(map[string][]*Predicate)

	// Pattern: table.column operator value
	// Only captures simple predicates with single table reference
	predPattern := regexp.MustCompile(
		`(?i)(\w+)\.(\w+)\s*(=|<|>|<=|>=|<>|LIKE|IN)\s*` +
			`('[^']*'|\d+(?:\.\d+)?|\([^)]+\))`)

	matches := predPattern.FindAllStringSubmatch(sqlQuery, -1)
	for _, match := range matches {
		if len(match) >= 5 {
			tableRef := match[1]
			column := match[2]
			operator := match[3]
			value := match[4]

			// Find the full table name for this reference
			tableName := a.resolveTableRef(tableRef, tables)
			if tableName == "" {
				continue
			}

			predicates[tableName] = append(predicates[tableName], &Predicate{
				Table:    tableName,
				Column:   column,
				Operator: operator,
				Value:    value,
				Raw:      match[0],
			})
		}
	}

	return predicates
}

// extractRequiredColumns extracts columns needed from each table.
func (a *Analyzer) extractRequiredColumns(
	sqlQuery string,
	tables []*TableRef,
	joins []*JoinCondition,
) map[string][]string {
	columns := make(map[string][]string)

	// Pattern: table.column references
	colPattern := regexp.MustCompile(`(?i)(\w+)\.(\w+)`)

	matches := colPattern.FindAllStringSubmatch(sqlQuery, -1)
	for _, match := range matches {
		if len(match) >= 3 {
			tableRef := match[1]
			column := match[2]

			tableName := a.resolveTableRef(tableRef, tables)
			if tableName == "" {
				continue
			}

			// Add column if not already present
			if !contains(columns[tableName], column) {
				columns[tableName] = append(columns[tableName], column)
			}
		}
	}

	// Ensure join keys are included
	for _, join := range joins {
		leftTable := a.resolveTableRef(join.LeftTable, tables)
		rightTable := a.resolveTableRef(join.RightTable, tables)

		if leftTable != "" && !contains(columns[leftTable], join.LeftCol) {
			columns[leftTable] = append(columns[leftTable], join.LeftCol)
		}
		if rightTable != "" && !contains(columns[rightTable], join.RightCol) {
			columns[rightTable] = append(columns[rightTable], join.RightCol)
		}
	}

	return columns
}

// extractAggregations extracts aggregate functions from SQL.
func (a *Analyzer) extractAggregations(sqlQuery string) []*Aggregation {
	var aggs []*Aggregation

	// Pattern: SUM(col), COUNT(*), AVG(col), etc.
	aggPattern := regexp.MustCompile(
		`(?i)(SUM|COUNT|AVG|MIN|MAX)\s*\(\s*(\*|[\w.]+)\s*\)(?:\s+(?:AS\s+)?(\w+))?`)

	matches := aggPattern.FindAllStringSubmatch(sqlQuery, -1)
	for _, match := range matches {
		if len(match) >= 3 {
			agg := &Aggregation{
				Function: strings.ToUpper(match[1]),
				Column:   match[2],
				Raw:      match[0],
			}
			if len(match) >= 4 && match[3] != "" {
				agg.Alias = match[3]
			}
			aggs = append(aggs, agg)
		}
	}

	return aggs
}

// extractOrderBy extracts ORDER BY clauses from SQL.
func (a *Analyzer) extractOrderBy(sqlQuery string) []*OrderByClause {
	var orderBy []*OrderByClause

	// Pattern: ORDER BY col [ASC|DESC]
	orderPattern := regexp.MustCompile(
		`(?i)ORDER\s+BY\s+(.+?)(?:\s+LIMIT|\s*$)`)

	match := orderPattern.FindStringSubmatch(sqlQuery)
	if len(match) >= 2 {
		// Split by comma
		parts := strings.Split(match[1], ",")
		for _, part := range parts {
			part = strings.TrimSpace(part)
			desc := strings.Contains(strings.ToUpper(part), " DESC")

			// Extract column name
			colPattern := regexp.MustCompile(`(?i)([\w.]+)\s*(?:ASC|DESC)?`)
			colMatch := colPattern.FindStringSubmatch(part)
			if len(colMatch) >= 2 {
				orderBy = append(orderBy, &OrderByClause{
					Column:     colMatch[1],
					Descending: desc,
				})
			}
		}
	}

	return orderBy
}

// extractLimit extracts LIMIT clause from SQL.
func (a *Analyzer) extractLimit(sqlQuery string) *int {
	limitPattern := regexp.MustCompile(`(?i)LIMIT\s+(\d+)`)

	match := limitPattern.FindStringSubmatch(sqlQuery)
	if len(match) >= 2 {
		var limit int
		fmt.Sscanf(match[1], "%d", &limit)
		return &limit
	}

	return nil
}

// resolveTableRef resolves an alias or name to a full table name.
func (a *Analyzer) resolveTableRef(ref string, tables []*TableRef) string {
	for _, table := range tables {
		if table.Alias == ref || table.Name == ref || table.FullName() == ref {
			return table.FullName()
		}
	}
	return ""
}

// defaultEngineForFormat returns the default engine for a table format.
func (a *Analyzer) defaultEngineForFormat(format string) string {
	switch strings.ToUpper(format) {
	case "ICEBERG":
		return "trino"
	case "DELTA":
		return "spark"
	case "HUDI":
		return "spark"
	case "PARQUET", "CSV", "ORC":
		return "duckdb"
	default:
		return "duckdb"
	}
}

// isKeyword checks if a string is a SQL keyword.
func isKeyword(s string) bool {
	keywords := map[string]bool{
		"ON": true, "JOIN": true, "WHERE": true, "AND": true, "OR": true,
		"FROM": true, "SELECT": true, "GROUP": true, "ORDER": true,
		"HAVING": true, "LIMIT": true, "INNER": true, "LEFT": true,
		"RIGHT": true, "FULL": true, "CROSS": true, "AS": true,
	}
	return keywords[strings.ToUpper(s)]
}

// contains checks if a slice contains a value.
func contains(slice []string, value string) bool {
	for _, v := range slice {
		if v == value {
			return true
		}
	}
	return false
}
