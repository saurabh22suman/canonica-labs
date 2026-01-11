// Package router provides engine selection and query routing.
// This file implements format-aware engine selection per phase-8-spec.md §7.
package router

import (
	"context"
	"fmt"
	"strings"

	"github.com/canonica-labs/canonica/internal/adapters"
	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/errors"
	"github.com/canonica-labs/canonica/internal/planner"
	"github.com/canonica-labs/canonica/internal/tables"
)

// TableFormat represents the underlying data format.
// Mirrors tables.StorageFormat but includes additional formats for Phase 8.
type TableFormat string

const (
	FormatIceberg TableFormat = "ICEBERG"
	FormatDelta   TableFormat = "DELTA"
	FormatHudi    TableFormat = "HUDI"
	FormatParquet TableFormat = "PARQUET"
	FormatORC     TableFormat = "ORC"
	FormatCSV     TableFormat = "CSV"
)

// EngineSelector selects the best engine for query execution.
// Per phase-8-spec.md §7: "Intelligently route queries to the best available engine."
type EngineSelector struct {
	router   *Router
	adapters map[string]adapters.EngineAdapter
}

// NewEngineSelector creates a new engine selector.
func NewEngineSelector(router *Router, engineAdapters map[string]adapters.EngineAdapter) *EngineSelector {
	return &EngineSelector{
		router:   router,
		adapters: engineAdapters,
	}
}

// SelectEngine selects the best engine for executing a plan.
// Per phase-8-spec.md §7.1:
//   - Rule 1: If table has explicit engine assignment, use it
//   - Rule 2: Select based on format capabilities
//   - Rule 3: Prefer engine by format
//   - Rule 4: Use first available
func (s *EngineSelector) SelectEngine(ctx context.Context, plan *planner.ExecutionPlan) (string, error) {
	if plan == nil || len(plan.ResolvedTables) == 0 {
		return "", errors.NewPlannerError("no tables in execution plan")
	}

	// Rule 1: If table has explicit engine assignment, use it
	for _, table := range plan.ResolvedTables {
		if len(table.Sources) > 0 && table.Sources[0].Engine != "" {
			engine := table.Sources[0].Engine
			if s.isEngineAvailable(engine) {
				return engine, nil
			}
			return "", fmt.Errorf("explicitly assigned engine %q is not available", engine)
		}
	}

	// Determine the format from the first table
	format := s.getTableFormat(plan.ResolvedTables[0])

	// Rule 2: Select based on format capabilities
	requiredCaps := plan.RequiredCapabilities
	candidates := s.findCapableEngines(format, requiredCaps)
	if len(candidates) == 0 {
		capStrings := make([]string, len(requiredCaps))
		for i, c := range requiredCaps {
			capStrings[i] = string(c)
		}
		return "", fmt.Errorf(
			"no engine available for format %s with capabilities %v",
			format, capStrings)
	}

	// Rule 3: Prefer engine by format
	preferred := s.preferredEngineForFormat(format)
	if s.contains(candidates, preferred) {
		return preferred, nil
	}

	// Rule 4: Use first available
	return candidates[0], nil
}

// SelectEngineForMultiTable selects an engine that can handle all tables.
// Per phase-8-spec.md §7.2: All tables must use the same engine (Phase 8 limitation).
func (s *EngineSelector) SelectEngineForMultiTable(
	ctx context.Context,
	plan *planner.ExecutionPlan,
) (string, error) {
	if plan == nil || len(plan.ResolvedTables) == 0 {
		return "", errors.NewPlannerError("no tables in execution plan")
	}

	// Track which engine each table prefers
	engines := make(map[string]bool)

	for _, table := range plan.ResolvedTables {
		// Create a single-table plan for engine selection
		singlePlan := &planner.ExecutionPlan{
			ResolvedTables:       []*tables.VirtualTable{table},
			RequiredCapabilities: plan.RequiredCapabilities,
		}

		engine, err := s.SelectEngine(ctx, singlePlan)
		if err != nil {
			return "", err
		}
		engines[engine] = true
	}

	// Phase 8 limitation: all tables must use the same engine
	// Phase 9: Returns ErrCrossEngineQuery for federation handling
	if len(engines) > 1 {
		var engineList []string
		for e := range engines {
			engineList = append(engineList, e)
		}
		return "", errors.NewCrossEngineQuery(engineList)
	}

	// Return the single engine
	for e := range engines {
		return e, nil
	}

	return "", errors.NewPlannerError("no engine available for query")
}

// preferredEngineForFormat returns the best engine for a given format.
// Per phase-8-spec.md §7.1:
//   - Iceberg → Trino (best Iceberg support)
//   - Delta → Spark (native Delta support)
//   - Hudi → Spark (Hudi is Spark-native)
//   - Parquet → DuckDB (fast for raw Parquet)
//   - Default → DuckDB
func (s *EngineSelector) preferredEngineForFormat(format TableFormat) string {
	switch format {
	case FormatIceberg:
		return "trino" // Trino has best Iceberg support
	case FormatDelta:
		return "spark" // Spark has native Delta support
	case FormatHudi:
		return "spark" // Hudi is Spark-native
	case FormatParquet:
		return "duckdb" // DuckDB is fast for raw Parquet
	case FormatORC:
		return "trino" // Trino has good ORC support
	case FormatCSV:
		return "duckdb" // DuckDB is efficient for CSV
	default:
		return "duckdb"
	}
}

// findCapableEngines returns engines that support the format and capabilities.
func (s *EngineSelector) findCapableEngines(format TableFormat, requiredCaps []capabilities.Capability) []string {
	var candidates []string

	for name, engine := range s.router.engines {
		// Skip unavailable engines
		if !engine.Available {
			continue
		}

		// Check if engine supports the format
		if !s.engineSupportsFormat(name, format) {
			continue
		}

		// Check if engine has all required capabilities
		if !engine.HasAllCapabilities(requiredCaps) {
			continue
		}

		candidates = append(candidates, name)
	}

	// Sort by priority
	s.sortByPriority(candidates)

	return candidates
}

// engineSupportsFormat checks if an engine can handle a given format.
func (s *EngineSelector) engineSupportsFormat(engine string, format TableFormat) bool {
	// Format support matrix per phase-8-spec.md
	switch engine {
	case "duckdb":
		// DuckDB supports Delta, Iceberg (via extensions), Parquet, CSV, ORC
		return format == FormatDelta ||
			format == FormatIceberg ||
			format == FormatParquet ||
			format == FormatCSV ||
			format == FormatORC
	case "trino":
		// Trino supports Iceberg, Delta, Hudi, Parquet, ORC
		return format == FormatIceberg ||
			format == FormatDelta ||
			format == FormatHudi ||
			format == FormatParquet ||
			format == FormatORC
	case "spark":
		// Spark supports all formats natively
		return format == FormatIceberg ||
			format == FormatDelta ||
			format == FormatHudi ||
			format == FormatParquet ||
			format == FormatORC ||
			format == FormatCSV
	case "snowflake":
		// Snowflake supports Iceberg tables, Parquet, CSV
		return format == FormatIceberg ||
			format == FormatParquet ||
			format == FormatCSV
	case "bigquery":
		// BigQuery supports Parquet, CSV
		return format == FormatParquet ||
			format == FormatCSV
	case "redshift":
		// Redshift supports Parquet via Spectrum
		return format == FormatParquet ||
			format == FormatCSV ||
			format == FormatORC
	default:
		return false
	}
}

// sortByPriority sorts engines by priority (lower = higher priority).
func (s *EngineSelector) sortByPriority(engines []string) {
	// Simple insertion sort since we typically have <10 engines
	for i := 1; i < len(engines); i++ {
		j := i
		for j > 0 {
			engineA, okA := s.router.GetEngine(engines[j-1])
			engineB, okB := s.router.GetEngine(engines[j])
			if okA && okB && engineA.Priority > engineB.Priority {
				engines[j-1], engines[j] = engines[j], engines[j-1]
			}
			j--
		}
	}
}

// getTableFormat extracts the format from a virtual table.
func (s *EngineSelector) getTableFormat(table *tables.VirtualTable) TableFormat {
	if len(table.Sources) == 0 {
		return FormatParquet // Default assumption
	}
	return TableFormat(table.Sources[0].Format)
}

// isEngineAvailable checks if an engine is registered and available.
func (s *EngineSelector) isEngineAvailable(name string) bool {
	engine, ok := s.router.GetEngine(name)
	return ok && engine.Available
}

// contains checks if a slice contains a value.
func (s *EngineSelector) contains(slice []string, value string) bool {
	for _, v := range slice {
		if v == value {
			return true
		}
	}
	return false
}

// FormatFromString converts a string to TableFormat.
func FormatFromString(s string) TableFormat {
	switch strings.ToUpper(s) {
	case "ICEBERG":
		return FormatIceberg
	case "DELTA":
		return FormatDelta
	case "HUDI":
		return FormatHudi
	case "PARQUET":
		return FormatParquet
	case "ORC":
		return FormatORC
	case "CSV":
		return FormatCSV
	default:
		return TableFormat(strings.ToUpper(s))
	}
}

// SupportsTimeTravel checks if a format supports time travel queries.
func SupportsTimeTravel(format TableFormat) bool {
	switch format {
	case FormatIceberg, FormatDelta, FormatHudi:
		return true
	default:
		return false
	}
}

// SupportsVersionQuery checks if a format supports version-based queries.
func SupportsVersionQuery(format TableFormat) bool {
	switch format {
	case FormatIceberg, FormatDelta:
		return true
	default:
		return false
	}
}
