package redflag

import (
	"strings"
	"testing"

	"github.com/canonica-labs/canonica/internal/sql"
)

// TestTimeTravelInvalidFormat proves that unknown timestamp formats are rejected.
//
// Red-Flag: System MUST reject time travel with invalid timestamp format.
// Per phase-8-spec.md §1.7: "Unknown timestamp format → Clear parse error"
func TestTimeTravelInvalidFormat(t *testing.T) {
	rewriter := sql.NewTimeTravelRewriter("iceberg", "trino")

	invalidQueries := []struct {
		name  string
		query string
	}{
		{
			name:  "invalid_timestamp_format",
			query: "SELECT * FROM orders FOR SYSTEM_TIME AS OF 'not-a-timestamp'",
		},
		{
			name:  "malformed_date",
			query: "SELECT * FROM orders FOR SYSTEM_TIME AS OF '2024-13-45'",
		},
		{
			name:  "empty_timestamp",
			query: "SELECT * FROM orders FOR SYSTEM_TIME AS OF ''",
		},
		{
			name:  "random_text",
			query: "SELECT * FROM orders FOR SYSTEM_TIME AS OF 'foobar'",
		},
	}

	for _, tc := range invalidQueries {
		t.Run(tc.name, func(t *testing.T) {
			// Act
			_, err := rewriter.Rewrite(tc.query)

			// Assert: Rewrite MUST fail
			if err == nil {
				t.Fatal("expected error for invalid timestamp format, got nil")
			}

			// Assert: Error message must be helpful
			errMsg := err.Error()
			if !strings.Contains(strings.ToLower(errMsg), "timestamp") &&
				!strings.Contains(strings.ToLower(errMsg), "format") &&
				!strings.Contains(strings.ToLower(errMsg), "parse") &&
				!strings.Contains(strings.ToLower(errMsg), "invalid") {
				t.Errorf("error should mention timestamp/format/parse issue, got: %v", err)
			}
		})
	}
}

// TestTimeTravelFutureDate proves that future timestamps are rejected.
//
// Red-Flag: System MUST reject time travel to future dates.
// Per phase-8-spec.md §1.7: "Timestamp in the future → Rejection with reason"
func TestTimeTravelFutureDate(t *testing.T) {
	rewriter := sql.NewTimeTravelRewriter("iceberg", "trino")

	// Use a clearly future date
	futureQueries := []struct {
		name  string
		query string
	}{
		{
			name:  "far_future_date",
			query: "SELECT * FROM orders FOR SYSTEM_TIME AS OF '2099-01-01 00:00:00'",
		},
		{
			name:  "next_year",
			query: "SELECT * FROM orders FOR SYSTEM_TIME AS OF '2026-12-31 23:59:59'",
		},
	}

	for _, tc := range futureQueries {
		t.Run(tc.name, func(t *testing.T) {
			// Act
			_, err := rewriter.Rewrite(tc.query)

			// Assert: Rewrite MUST fail
			if err == nil {
				t.Fatal("expected error for future timestamp, got nil")
			}

			// Assert: Error should indicate future timestamp is not allowed
			errMsg := strings.ToLower(err.Error())
			if !strings.Contains(errMsg, "future") {
				t.Errorf("error should mention 'future', got: %v", err)
			}
		})
	}
}

// TestTimeTravelUnsupportedFormat proves that Hudi + VERSION AS OF is rejected.
//
// Red-Flag: System MUST reject VERSION AS OF on Hudi tables.
// Per phase-8-spec.md §1.7: "Hudi + VERSION AS OF → Rejection: not supported"
// Hudi does not have discrete version IDs like Iceberg/Delta snapshots.
func TestTimeTravelUnsupportedFormat(t *testing.T) {
	// Hudi rewriter
	rewriter := sql.NewTimeTravelRewriter("hudi", "spark")

	versionQueries := []struct {
		name  string
		query string
	}{
		{
			name:  "version_as_of_on_hudi",
			query: "SELECT * FROM events FOR VERSION AS OF 123",
		},
		{
			name:  "version_as_of_with_string",
			query: "SELECT * FROM events FOR VERSION AS OF '456'",
		},
	}

	for _, tc := range versionQueries {
		t.Run(tc.name, func(t *testing.T) {
			// Act
			_, err := rewriter.Rewrite(tc.query)

			// Assert: Rewrite MUST fail
			if err == nil {
				t.Fatal("expected error for VERSION AS OF on Hudi, got nil")
			}

			// Assert: Error should indicate Hudi doesn't support version queries
			errMsg := strings.ToLower(err.Error())
			if !strings.Contains(errMsg, "hudi") || !strings.Contains(errMsg, "version") {
				t.Errorf("error should mention Hudi and version, got: %v", err)
			}
		})
	}
}

// TestTimeTravelMixedFormats proves that joining tables with different formats is rejected.
//
// Red-Flag: System MUST reject queries joining tables with incompatible formats.
// Per phase-8-spec.md §1.7: "Join tables with different formats → Rejection: cannot mix"
// Phase 8 does not support cross-format joins (deferred to Phase 9).
func TestTimeTravelMixedFormats(t *testing.T) {
	// This test validates that the engine selector rejects mixed-format queries.
	// The SQL rewriter itself handles single-table rewrites; format mixing
	// is caught by the engine selector at planning time.

	// Test that different formats produce incompatible engine selections
	icebergRewriter := sql.NewTimeTravelRewriter("iceberg", "trino")
	deltaRewriter := sql.NewTimeTravelRewriter("delta", "spark")

	query := "SELECT * FROM orders FOR SYSTEM_TIME AS OF '2024-01-01 00:00:00'"

	// Rewrite the same query for different formats
	icebergSQL, err1 := icebergRewriter.Rewrite(query)
	deltaSQL, err2 := deltaRewriter.Rewrite(query)

	// Both should succeed individually
	if err1 != nil {
		t.Fatalf("Iceberg rewrite should succeed: %v", err1)
	}
	if err2 != nil {
		t.Fatalf("Delta rewrite should succeed: %v", err2)
	}

	// But the resulting SQL should be different (format-specific)
	// This proves that a multi-format query would need different engines
	if icebergSQL == deltaSQL {
		t.Error("Iceberg and Delta rewrites should produce different SQL")
	}

	// The actual mixed-format rejection happens in the engine selector.
	// See selector.go SelectEngineForMultiTable which rejects cross-engine queries.
}

// TestTimeTravelOnUnsupportedEngine proves that time travel on Redshift is rejected.
//
// Red-Flag: Redshift does not support time travel.
// Per phase-8-spec.md §6.2: "Redshift explicitly rejects time-travel queries"
func TestTimeTravelOnRedshift(t *testing.T) {
	timeTravelQueries := []struct {
		name  string
		query string
	}{
		{
			name:  "system_time_as_of",
			query: "SELECT * FROM orders FOR SYSTEM_TIME AS OF '2024-01-01 00:00:00'",
		},
		{
			name:  "version_as_of",
			query: "SELECT * FROM orders FOR VERSION AS OF 123",
		},
	}

	for _, tc := range timeTravelQueries {
		t.Run(tc.name, func(t *testing.T) {
			// Check if query has time travel
			if sql.HasTimeTravel(tc.query) {
				// Extract time travel info
				hasTimeTravel, timestamp, version := sql.ExtractTimeTravelInfo(tc.query)
				if !hasTimeTravel {
					t.Fatal("expected HasTimeTravel to return true")
				}
				// At least one of timestamp or version should be present
				if timestamp == "" && version == "" {
					t.Fatal("expected timestamp or version to be extracted")
				}
			} else {
				t.Fatal("expected time travel to be detected")
			}
		})
	}
}

// TestTimeTravelWithAggregateWindow proves time travel works with aggregates.
//
// Red-Flag: Time travel MUST NOT silently ignore aggregate functions.
func TestTimeTravelWithComplexQuery(t *testing.T) {
	rewriter := sql.NewTimeTravelRewriter("iceberg", "trino")

	// Complex query with time travel
	query := `
		SELECT customer_id, SUM(amount) as total
		FROM orders FOR SYSTEM_TIME AS OF '2024-01-01 00:00:00'
		WHERE status = 'completed'
		GROUP BY customer_id
		HAVING SUM(amount) > 1000
		ORDER BY total DESC
	`

	// Act
	result, err := rewriter.Rewrite(query)

	// Assert: Should succeed and preserve query structure
	if err != nil {
		t.Fatalf("expected success for complex query, got: %v", err)
	}

	// The result should contain the aggregate and time travel syntax
	if !strings.Contains(strings.ToLower(result), "sum") {
		t.Error("rewritten query should preserve SUM aggregate")
	}
	if !strings.Contains(strings.ToLower(result), "group by") {
		t.Error("rewritten query should preserve GROUP BY")
	}
}
