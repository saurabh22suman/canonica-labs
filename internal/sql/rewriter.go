// Package sql provides SQL rewriting for format-specific syntax translation.
//
// Per phase-8-spec.md §1: Unified Time-Travel Syntax
// Users write standard SQL with FOR SYSTEM_TIME AS OF or FOR VERSION AS OF,
// and Canonic translates it to the correct format-specific syntax.
package sql

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/canonica-labs/canonica/internal/catalog"
)

// TimeTravelClause represents a parsed time-travel clause.
type TimeTravelClause struct {
	// TableName is the table this clause applies to.
	TableName string

	// ClauseType is either "SYSTEM_TIME" or "VERSION".
	ClauseType string

	// Timestamp is the timestamp value (for SYSTEM_TIME).
	Timestamp string

	// Version is the version/snapshot ID (for VERSION).
	Version string

	// OriginalClause is the full original clause text.
	OriginalClause string
}

// TimeTravelRewriter rewrites unified time-travel syntax to format/engine-specific syntax.
// Per phase-8-spec.md §1.3: SQL Rewriter for format transparency.
type TimeTravelRewriter struct {
	format catalog.TableFormat
	engine string
}

// NewTimeTravelRewriter creates a new rewriter for the given format and engine.
func NewTimeTravelRewriter(format catalog.TableFormat, engine string) *TimeTravelRewriter {
	return &TimeTravelRewriter{
		format: format,
		engine: engine,
	}
}

// Patterns for detecting time-travel clauses.
var (
	// FOR SYSTEM_TIME AS OF 'timestamp' or FOR SYSTEM_TIME AS OF timestamp
	systemTimePattern = regexp.MustCompile(
		`(?i)\s+FOR\s+SYSTEM_TIME\s+AS\s+OF\s+('([^']+)'|"([^"]+)"|(\S+))`)

	// FOR VERSION AS OF version_id
	versionAsOfPattern = regexp.MustCompile(
		`(?i)\s+FOR\s+VERSION\s+AS\s+OF\s+(\d+|'[^']+')`)
)

// Rewrite translates unified time-travel syntax to format/engine-specific syntax.
// Per phase-8-spec.md §1.3: Transparent format-specific translation.
func (r *TimeTravelRewriter) Rewrite(sql string) (string, error) {
	// Detect time-travel clauses
	clauses := r.extractTimeTravelClauses(sql)
	if len(clauses) == 0 {
		// No time-travel, return as-is
		return sql, nil
	}

	// Validate time-travel is supported for this format
	if err := r.validateTimeTravelSupport(clauses); err != nil {
		return "", err
	}

	// Rewrite each clause
	result := sql
	for _, clause := range clauses {
		rewritten, err := r.rewriteClause(clause)
		if err != nil {
			return "", err
		}
		result = strings.Replace(result, clause.OriginalClause, rewritten, 1)
	}

	return result, nil
}

// extractTimeTravelClauses finds all time-travel clauses in the SQL.
func (r *TimeTravelRewriter) extractTimeTravelClauses(sql string) []TimeTravelClause {
	var clauses []TimeTravelClause

	// Find SYSTEM_TIME AS OF clauses
	matches := systemTimePattern.FindAllStringSubmatch(sql, -1)
	for _, match := range matches {
		if len(match) >= 2 {
			// Extract timestamp from the match
			timestamp := match[1]
			// Remove quotes if present
			timestamp = strings.Trim(timestamp, "'\"")

			clauses = append(clauses, TimeTravelClause{
				ClauseType:     "SYSTEM_TIME",
				Timestamp:      timestamp,
				OriginalClause: match[0],
			})
		}
	}

	// Find VERSION AS OF clauses
	matches = versionAsOfPattern.FindAllStringSubmatch(sql, -1)
	for _, match := range matches {
		if len(match) >= 2 {
			version := strings.Trim(match[1], "'")
			clauses = append(clauses, TimeTravelClause{
				ClauseType:     "VERSION",
				Version:        version,
				OriginalClause: match[0],
			})
		}
	}

	return clauses
}

// validateTimeTravelSupport checks if time-travel is supported for this format/engine.
// Per phase-8-spec.md §1.7: Red-Flag behavior for unsupported combinations.
func (r *TimeTravelRewriter) validateTimeTravelSupport(clauses []TimeTravelClause) error {
	for _, clause := range clauses {
		// Check VERSION AS OF support
		if clause.ClauseType == "VERSION" {
			switch r.format {
			case catalog.FormatHudi:
				// Per phase-8-spec.md: Hudi does not support VERSION AS OF
				return fmt.Errorf(
					"time-travel: VERSION AS OF is not supported for Hudi tables; "+
						"use FOR SYSTEM_TIME AS OF with a timestamp instead")
			case catalog.FormatParquet, catalog.FormatCSV, catalog.FormatORC:
				return fmt.Errorf(
					"time-travel: VERSION AS OF is not supported for %s tables; "+
						"raw files do not have version history",
					r.format)
			}
		}

		// Check SYSTEM_TIME AS OF support
		if clause.ClauseType == "SYSTEM_TIME" {
			switch r.format {
			case catalog.FormatParquet, catalog.FormatCSV, catalog.FormatORC:
				return fmt.Errorf(
					"time-travel: FOR SYSTEM_TIME AS OF is not supported for %s tables; "+
						"raw files do not have time-travel capability",
					r.format)
			}

			// Validate timestamp format and value
			if err := r.validateTimestamp(clause.Timestamp); err != nil {
				return err
			}
		}
	}

	return nil
}

// validateTimestamp validates the timestamp format and value.
// Per phase-8-spec.md §1.7: Reject invalid and future timestamps.
func (r *TimeTravelRewriter) validateTimestamp(ts string) error {
	if ts == "" {
		return fmt.Errorf("time-travel: empty timestamp not allowed")
	}

	// Try to parse common timestamp formats
	formats := []string{
		time.RFC3339,
		"2006-01-02T15:04:05Z",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}

	var parsedTime time.Time
	var parseErr error

	for _, format := range formats {
		parsedTime, parseErr = time.Parse(format, ts)
		if parseErr == nil {
			break
		}
	}

	if parseErr != nil {
		return fmt.Errorf(
			"time-travel: invalid timestamp format %q; "+
				"expected ISO 8601 format (e.g., '2026-01-01T00:00:00Z')",
			ts)
	}

	// Reject future timestamps
	if parsedTime.After(time.Now()) {
		return fmt.Errorf(
			"time-travel: timestamp %q is in the future; "+
				"time-travel can only query historical data",
			ts)
	}

	return nil
}

// rewriteClause rewrites a single time-travel clause to format/engine-specific syntax.
func (r *TimeTravelRewriter) rewriteClause(clause TimeTravelClause) (string, error) {
	switch clause.ClauseType {
	case "SYSTEM_TIME":
		return r.rewriteSystemTime(clause)
	case "VERSION":
		return r.rewriteVersion(clause)
	default:
		return "", fmt.Errorf("time-travel: unknown clause type %q", clause.ClauseType)
	}
}

// rewriteSystemTime rewrites FOR SYSTEM_TIME AS OF to format/engine-specific syntax.
// Per phase-8-spec.md §1.2: Format-Specific Translation Table.
func (r *TimeTravelRewriter) rewriteSystemTime(clause TimeTravelClause) (string, error) {
	ts := clause.Timestamp

	switch r.format {
	case catalog.FormatIceberg:
		return r.rewriteIcebergSystemTime(ts)
	case catalog.FormatDelta:
		return r.rewriteDeltaSystemTime(ts)
	case catalog.FormatHudi:
		return r.rewriteHudiSystemTime(ts)
	default:
		// Unknown format - pass through (will likely fail at engine level)
		return clause.OriginalClause, nil
	}
}

// rewriteIcebergSystemTime translates to Iceberg-specific syntax.
// Per phase-8-spec.md §1.4: Iceberg Time-Travel Translation.
// Per T033: Engines use different time-travel syntax.
func (r *TimeTravelRewriter) rewriteIcebergSystemTime(ts string) (string, error) {
	switch r.engine {
	case "trino":
		// Trino Iceberg: FOR TIMESTAMP AS OF TIMESTAMP 'ts'
		return fmt.Sprintf(" FOR TIMESTAMP AS OF TIMESTAMP '%s'", ts), nil
	case "spark":
		// Spark Iceberg: TIMESTAMP AS OF 'ts'
		return fmt.Sprintf(" TIMESTAMP AS OF '%s'", ts), nil
	case "duckdb":
		// DuckDB Iceberg: Uses iceberg_scan with snapshot_id parameter
		// For time-travel, DuckDB needs: SELECT * FROM iceberg_scan('path', at_snapshot_id=X)
		// Since we can't easily rewrite to function call, use the closest SQL syntax
		return fmt.Sprintf(" AT TIMESTAMP '%s'", ts), nil
	default:
		// Default to Trino syntax
		return fmt.Sprintf(" FOR TIMESTAMP AS OF TIMESTAMP '%s'", ts), nil
	}
}

// rewriteDeltaSystemTime translates to Delta-specific syntax.
// Per phase-8-spec.md §1.5: Delta Time-Travel Translation.
// Per T033: Engines use different time-travel syntax.
func (r *TimeTravelRewriter) rewriteDeltaSystemTime(ts string) (string, error) {
	switch r.engine {
	case "spark":
		// Spark Delta: TIMESTAMP AS OF 'ts'
		return fmt.Sprintf(" TIMESTAMP AS OF '%s'", ts), nil
	case "trino":
		// Trino Delta: connector-specific
		return fmt.Sprintf(" TIMESTAMP AS OF '%s'", ts), nil
	case "duckdb":
		// DuckDB Delta: Uses delta_scan with timestamp parameter
		return fmt.Sprintf(" AT TIMESTAMP '%s'", ts), nil
	default:
		return fmt.Sprintf(" TIMESTAMP AS OF '%s'", ts), nil
	}
}

// rewriteHudiSystemTime translates to Hudi-specific syntax.
// Per phase-8-spec.md §1.6: Hudi Time-Travel Translation.
func (r *TimeTravelRewriter) rewriteHudiSystemTime(ts string) (string, error) {
	switch r.engine {
	case "spark":
		// Spark Hudi: Use read options (requires special handling)
		// For now, return a compatible syntax
		return fmt.Sprintf(" TIMESTAMP AS OF '%s'", ts), nil
	case "trino":
		// Trino Hudi: connector-specific
		return fmt.Sprintf(" TIMESTAMP AS OF '%s'", ts), nil
	default:
		return fmt.Sprintf(" TIMESTAMP AS OF '%s'", ts), nil
	}
}

// rewriteVersion rewrites FOR VERSION AS OF to format/engine-specific syntax.
func (r *TimeTravelRewriter) rewriteVersion(clause TimeTravelClause) (string, error) {
	version := clause.Version

	switch r.format {
	case catalog.FormatIceberg:
		return r.rewriteIcebergVersion(version)
	case catalog.FormatDelta:
		return r.rewriteDeltaVersion(version)
	default:
		return "", fmt.Errorf(
			"time-travel: VERSION AS OF not supported for format %s",
			r.format)
	}
}

// rewriteIcebergVersion translates VERSION AS OF to Iceberg-specific syntax.
// Per phase-8-spec.md §1.4: Iceberg supports snapshot IDs.
func (r *TimeTravelRewriter) rewriteIcebergVersion(version string) (string, error) {
	switch r.engine {
	case "trino":
		// Trino: FOR VERSION AS OF snapshot_id
		return fmt.Sprintf(" FOR VERSION AS OF %s", version), nil
	case "spark":
		// Spark: VERSION AS OF snapshot_id
		return fmt.Sprintf(" VERSION AS OF %s", version), nil
	default:
		return fmt.Sprintf(" FOR VERSION AS OF %s", version), nil
	}
}

// rewriteDeltaVersion translates VERSION AS OF to Delta-specific syntax.
// Per phase-8-spec.md §1.5: Delta supports version numbers.
func (r *TimeTravelRewriter) rewriteDeltaVersion(version string) (string, error) {
	switch r.engine {
	case "spark":
		// Spark Delta: VERSION AS OF version
		return fmt.Sprintf(" VERSION AS OF %s", version), nil
	case "trino":
		// Trino Delta: connector-specific
		return fmt.Sprintf(" VERSION AS OF %s", version), nil
	default:
		return fmt.Sprintf(" VERSION AS OF %s", version), nil
	}
}

// WarehouseRewriter rewrites time-travel for cloud warehouses.
// Per phase-8-spec.md §4-6: Snowflake, BigQuery, Redshift adapters.
type WarehouseRewriter struct {
	warehouse string
}

// NewWarehouseRewriter creates a rewriter for a specific warehouse.
func NewWarehouseRewriter(warehouse string) *WarehouseRewriter {
	return &WarehouseRewriter{warehouse: warehouse}
}

// Rewrite translates time-travel syntax for the warehouse.
func (r *WarehouseRewriter) Rewrite(sql string) (string, error) {
	clauses := (&TimeTravelRewriter{}).extractTimeTravelClauses(sql)
	if len(clauses) == 0 {
		return sql, nil
	}

	result := sql
	for _, clause := range clauses {
		rewritten, err := r.rewriteClause(clause)
		if err != nil {
			return "", err
		}
		result = strings.Replace(result, clause.OriginalClause, rewritten, 1)
	}

	return result, nil
}

// rewriteClause rewrites a time-travel clause for the warehouse.
func (r *WarehouseRewriter) rewriteClause(clause TimeTravelClause) (string, error) {
	switch r.warehouse {
	case "snowflake":
		return r.rewriteSnowflake(clause)
	case "bigquery":
		return r.rewriteBigQuery(clause)
	case "redshift":
		return "", fmt.Errorf(
			"time-travel: Redshift does not support time-travel queries; "+
				"consider using a table with historical data or a different warehouse")
	default:
		return "", fmt.Errorf("time-travel: unknown warehouse %q", r.warehouse)
	}
}

// rewriteSnowflake translates to Snowflake AT() syntax.
// Per phase-8-spec.md §4.2: Snowflake time-travel.
func (r *WarehouseRewriter) rewriteSnowflake(clause TimeTravelClause) (string, error) {
	if clause.ClauseType == "SYSTEM_TIME" {
		// Snowflake: AT(TIMESTAMP => 'ts'::TIMESTAMP)
		return fmt.Sprintf(" AT(TIMESTAMP => '%s'::TIMESTAMP)", clause.Timestamp), nil
	}
	// Snowflake doesn't support VERSION AS OF
	return "", fmt.Errorf(
		"time-travel: Snowflake does not support VERSION AS OF; "+
			"use FOR SYSTEM_TIME AS OF instead")
}

// rewriteBigQuery translates to BigQuery syntax.
// Per phase-8-spec.md §5.2: BigQuery uses similar syntax to Canonic.
func (r *WarehouseRewriter) rewriteBigQuery(clause TimeTravelClause) (string, error) {
	if clause.ClauseType == "SYSTEM_TIME" {
		// BigQuery: FOR SYSTEM_TIME AS OF TIMESTAMP 'ts'
		return fmt.Sprintf(" FOR SYSTEM_TIME AS OF TIMESTAMP '%s'", clause.Timestamp), nil
	}
	// BigQuery doesn't support VERSION AS OF
	return "", fmt.Errorf(
		"time-travel: BigQuery does not support VERSION AS OF; "+
			"use FOR SYSTEM_TIME AS OF instead")
}

// HasTimeTravel checks if the SQL contains time-travel clauses.
func HasTimeTravel(sql string) bool {
	return systemTimePattern.MatchString(sql) || versionAsOfPattern.MatchString(sql)
}

// ExtractTimeTravelInfo extracts time-travel information from SQL.
func ExtractTimeTravelInfo(sql string) (hasTimeTravel bool, timestamp string, version string) {
	rewriter := &TimeTravelRewriter{}
	clauses := rewriter.extractTimeTravelClauses(sql)

	for _, clause := range clauses {
		hasTimeTravel = true
		if clause.ClauseType == "SYSTEM_TIME" {
			timestamp = clause.Timestamp
		} else if clause.ClauseType == "VERSION" {
			version = clause.Version
		}
	}

	return
}
