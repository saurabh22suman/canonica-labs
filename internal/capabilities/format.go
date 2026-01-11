// Package capabilities provides format-aware capability mapping.
//
// Per phase-8-spec.md §2: Format-Aware Capability Mapping
// Capabilities are mapped based on table format, not just engine.
package capabilities

import (
	"github.com/canonica-labs/canonica/internal/catalog"
)

// FormatCapabilities maps table formats to their supported capabilities.
// Per phase-8-spec.md §2.1: Format Capabilities definition.
var FormatCapabilities = map[catalog.TableFormat][]Capability{
	catalog.FormatIceberg: {
		CapabilityRead,
		CapabilityAggregate,
		CapabilityFilter,
		CapabilityTimeTravel,
		CapabilitySnapshotQuery,   // Query specific snapshot
		CapabilityVersionQuery,    // Query specific version
		CapabilitySchemaEvolution, // Read old schemas
		CapabilityPartitionPruning,
		CapabilityWindow,
		CapabilityCTE,
	},
	catalog.FormatDelta: {
		CapabilityRead,
		CapabilityAggregate,
		CapabilityFilter,
		CapabilityTimeTravel,
		CapabilityVersionQuery,    // Query specific version
		CapabilitySchemaEvolution, // Read old schemas
		CapabilityPartitionPruning,
		CapabilityWindow,
		CapabilityCTE,
	},
	catalog.FormatHudi: {
		CapabilityRead,
		CapabilityAggregate,
		CapabilityFilter,
		CapabilityTimeTravel,       // Limited time-travel
		CapabilityIncrementalQuery, // Read changes since timestamp
		CapabilityPartitionPruning,
		CapabilityWindow,
	},
	catalog.FormatParquet: {
		CapabilityRead,
		CapabilityAggregate,
		CapabilityFilter,
		CapabilityWindow,
		// No time-travel for raw Parquet
	},
	catalog.FormatORC: {
		CapabilityRead,
		CapabilityAggregate,
		CapabilityFilter,
		CapabilityWindow,
		// No time-travel for raw ORC
	},
	catalog.FormatCSV: {
		CapabilityRead,
		CapabilityAggregate,
		CapabilityFilter,
		// Limited capabilities for CSV
	},
	catalog.FormatUnknown: {
		CapabilityRead,
		CapabilityFilter,
	},
}

// GetFormatCapabilities returns the capabilities for a table format.
func GetFormatCapabilities(format catalog.TableFormat) []Capability {
	caps, ok := FormatCapabilities[format]
	if !ok {
		return FormatCapabilities[catalog.FormatUnknown]
	}
	return caps
}

// FormatSupportsCapability checks if a format supports a specific capability.
func FormatSupportsCapability(format catalog.TableFormat, cap Capability) bool {
	caps := GetFormatCapabilities(format)
	for _, c := range caps {
		if c == cap {
			return true
		}
	}
	return false
}

// FormatSupportsTimeTravel checks if a format supports time-travel queries.
// Per phase-8-spec.md §2.2: Capability Validation for time-travel.
func FormatSupportsTimeTravel(format catalog.TableFormat) bool {
	return FormatSupportsCapability(format, CapabilityTimeTravel)
}

// FormatSupportsVersionQuery checks if a format supports VERSION AS OF queries.
// Per phase-8-spec.md §2.2: Capability Validation for version queries.
func FormatSupportsVersionQuery(format catalog.TableFormat) bool {
	return FormatSupportsCapability(format, CapabilityVersionQuery)
}

// FormatSupportsSnapshotQuery checks if a format supports snapshot queries.
// Per phase-8-spec.md §2.2: Capability Validation for snapshot queries.
func FormatSupportsSnapshotQuery(format catalog.TableFormat) bool {
	return FormatSupportsCapability(format, CapabilitySnapshotQuery)
}

// EngineCapabilities maps engines to their supported capabilities.
// This complements FormatCapabilities - the effective capabilities
// are the intersection of format and engine capabilities.
var EngineCapabilities = map[string][]Capability{
	"trino": {
		CapabilityRead,
		CapabilityAggregate,
		CapabilityFilter,
		CapabilityTimeTravel,
		CapabilitySnapshotQuery,
		CapabilityVersionQuery,
		CapabilitySchemaEvolution,
		CapabilityPartitionPruning,
		CapabilityWindow,
		CapabilityCTE,
	},
	"spark": {
		CapabilityRead,
		CapabilityAggregate,
		CapabilityFilter,
		CapabilityTimeTravel,
		CapabilitySnapshotQuery,
		CapabilityVersionQuery,
		CapabilitySchemaEvolution,
		CapabilityPartitionPruning,
		CapabilityIncrementalQuery,
		CapabilityWindow,
		CapabilityCTE,
	},
	"duckdb": {
		CapabilityRead,
		CapabilityAggregate,
		CapabilityFilter,
		CapabilityWindow,
		CapabilityCTE,
		// DuckDB doesn't support lakehouse time-travel natively
	},
	"snowflake": {
		CapabilityRead,
		CapabilityAggregate,
		CapabilityFilter,
		CapabilityTimeTravel, // Snowflake supports up to 90 days
		CapabilityWindow,
		CapabilityCTE,
	},
	"bigquery": {
		CapabilityRead,
		CapabilityAggregate,
		CapabilityFilter,
		CapabilityTimeTravel, // BigQuery supports up to 7 days
		CapabilityWindow,
		CapabilityCTE,
	},
	"redshift": {
		CapabilityRead,
		CapabilityAggregate,
		CapabilityFilter,
		CapabilityWindow,
		// NO CapabilityTimeTravel - Redshift doesn't support it
	},
}

// GetEngineCapabilities returns the capabilities for an engine.
func GetEngineCapabilities(engine string) []Capability {
	caps, ok := EngineCapabilities[engine]
	if !ok {
		return []Capability{CapabilityRead, CapabilityFilter}
	}
	return caps
}

// EngineSupportsCapability checks if an engine supports a specific capability.
func EngineSupportsCapability(engine string, cap Capability) bool {
	caps := GetEngineCapabilities(engine)
	for _, c := range caps {
		if c == cap {
			return true
		}
	}
	return false
}

// EffectiveCapabilities returns the capabilities that both the format and engine support.
// This is the intersection of format and engine capabilities.
func EffectiveCapabilities(format catalog.TableFormat, engine string) []Capability {
	formatCaps := NewCapabilitySet(GetFormatCapabilities(format))
	engineCaps := NewCapabilitySet(GetEngineCapabilities(engine))

	var result []Capability
	for cap := range formatCaps {
		if engineCaps.Has(cap) {
			result = append(result, cap)
		}
	}
	return result
}

// CanExecute checks if a query requiring certain capabilities can be executed
// on the given format/engine combination.
func CanExecute(format catalog.TableFormat, engine string, required []Capability) (bool, []Capability) {
	effective := NewCapabilitySet(EffectiveCapabilities(format, engine))
	var missing []Capability

	for _, req := range required {
		if !effective.Has(req) {
			missing = append(missing, req)
		}
	}

	return len(missing) == 0, missing
}
