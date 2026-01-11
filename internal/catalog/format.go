// Package catalog provides format detection for lakehouse table formats.
//
// Per phase-7-spec.md §5: Automatically detect table format from storage location and metadata.
package catalog

import (
	"strings"
)

// DetectFormatFromProperties detects table format from metadata properties.
// Per phase-7-spec.md §2.2: Detect Iceberg/Delta/Hudi from Hive table properties.
func DetectFormatFromProperties(properties map[string]string, serdeLib string) TableFormat {
	if properties == nil {
		return detectFromSerde(serdeLib)
	}

	// Check table_type property (common pattern)
	tableType := strings.ToUpper(properties["table_type"])
	switch tableType {
	case "ICEBERG":
		return FormatIceberg
	case "DELTA":
		return FormatDelta
	case "HUDI":
		return FormatHudi
	}

	// Check for Iceberg-specific properties
	if _, ok := properties["metadata_location"]; ok {
		return FormatIceberg
	}

	// Check for Delta
	if properties["spark.sql.sources.provider"] == "delta" {
		return FormatDelta
	}
	if strings.Contains(properties["location"], "_delta_log") {
		return FormatDelta
	}

	// Check for Hudi
	if properties["spark.sql.sources.provider"] == "hudi" {
		return FormatHudi
	}
	if _, ok := properties["hoodie.table.name"]; ok {
		return FormatHudi
	}

	// Fall back to SerDe detection
	return detectFromSerde(serdeLib)
}

// detectFromSerde detects format from Hive SerDe library name.
func detectFromSerde(serdeLib string) TableFormat {
	if serdeLib == "" {
		return FormatUnknown
	}

	serdeLib = strings.ToLower(serdeLib)

	switch {
	// Check Iceberg first (before parquet since some Iceberg serdes contain "parquet")
	case strings.Contains(serdeLib, "iceberg"):
		return FormatIceberg
	// Check Delta
	case strings.Contains(serdeLib, "delta"):
		return FormatDelta
	// Check Hudi (before parquet since Hudi serde may contain "parquet")
	case strings.Contains(serdeLib, "hudi") || strings.Contains(serdeLib, "hoodie"):
		return FormatHudi
	// Standard formats
	case strings.Contains(serdeLib, "parquet"):
		return FormatParquet
	case strings.Contains(serdeLib, "orc"):
		return FormatORC
	case strings.Contains(serdeLib, "csv"):
		return FormatCSV
	default:
		return FormatUnknown
	}
}

// DetectFormatFromLocation detects table format from storage location.
// Per phase-7-spec.md §5.1: Check for format-specific directories.
//
// This function requires a PathChecker to inspect the storage location.
// Use DetectFormatFromLocationPath for synchronous file system checks.
func DetectFormatFromLocation(location string, pathChecker PathChecker) (TableFormat, error) {
	// Check for Iceberg metadata
	icebergMetadata := location + "/metadata"
	if exists, err := pathChecker.Exists(icebergMetadata); err != nil {
		return FormatUnknown, err
	} else if exists {
		return FormatIceberg, nil
	}

	// Check for Delta log
	deltaLog := location + "/_delta_log"
	if exists, err := pathChecker.Exists(deltaLog); err != nil {
		return FormatUnknown, err
	} else if exists {
		return FormatDelta, nil
	}

	// Check for Hudi metadata
	hudiMetadata := location + "/.hoodie"
	if exists, err := pathChecker.Exists(hudiMetadata); err != nil {
		return FormatUnknown, err
	} else if exists {
		return FormatHudi, nil
	}

	return FormatUnknown, nil
}

// PathChecker checks if paths exist in storage.
// Per phase-7-spec.md: Abstract storage access for format detection.
type PathChecker interface {
	// Exists returns true if the path exists.
	Exists(path string) (bool, error)
}

// DetectFormatFromLocationHint provides a fast format guess from location string.
// This does NOT access storage; it only checks the path string.
func DetectFormatFromLocationHint(location string) TableFormat {
	if location == "" {
		return FormatUnknown
	}

	location = strings.ToLower(location)

	// Check for Delta log in path
	if strings.Contains(location, "_delta_log") {
		return FormatDelta
	}

	// Check for Hudi metadata in path
	if strings.Contains(location, ".hoodie") {
		return FormatHudi
	}

	// Check for Iceberg metadata in path
	if strings.Contains(location, "/metadata/") || strings.HasSuffix(location, "/metadata") {
		return FormatIceberg
	}

	// Check file extension hints
	if strings.HasSuffix(location, ".parquet") {
		return FormatParquet
	}
	if strings.HasSuffix(location, ".orc") {
		return FormatORC
	}
	if strings.HasSuffix(location, ".csv") {
		return FormatCSV
	}

	return FormatUnknown
}

// SelectEngine chooses the query engine based on table format.
// Per phase-7-spec.md §4.3: Engine selection based on format.
func SelectEngine(format TableFormat) string {
	switch format {
	case FormatIceberg:
		return "trino" // Trino has best Iceberg support
	case FormatDelta:
		return "trino" // Trino Delta connector
	case FormatHudi:
		return "trino" // Trino Hudi connector
	case FormatParquet:
		return "duckdb" // DuckDB is fast for Parquet
	case FormatORC:
		return "trino" // DuckDB has limited ORC support
	case FormatCSV:
		return "duckdb" // DuckDB is fast for CSV
	default:
		return "duckdb" // Default fallback
	}
}
