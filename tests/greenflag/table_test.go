package greenflag

import (
	"testing"

	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/tables"
)

// TestTable_ValidDefinition proves that valid table definitions pass validation.
//
// Green-Flag: Well-formed table definitions MUST pass validation.
func TestTable_ValidDefinition(t *testing.T) {
	// Arrange: Valid table
	vt := &tables.VirtualTable{
		Name:        "valid_table",
		Description: "A test table",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatDelta, Location: "s3://bucket/delta"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
		Constraints:  []capabilities.Constraint{capabilities.ConstraintReadOnly},
	}

	// Act
	err := vt.Validate()

	// Assert: Validation MUST succeed
	if err != nil {
		t.Fatalf("expected valid table to pass validation, got error: %v", err)
	}
}

// TestTable_MultipleFormats proves that tables can have multiple format sources.
//
// Green-Flag: Tables MAY have sources in different formats.
func TestTable_MultipleFormats(t *testing.T) {
	// Arrange: Table with multiple format sources (different formats)
	vt := &tables.VirtualTable{
		Name: "multi_format_table",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatDelta, Location: "s3://bucket/delta"},
			{Format: tables.FormatIceberg, Location: "s3://bucket/iceberg"},
			{Format: tables.FormatParquet, Location: "s3://bucket/parquet"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	}

	// Act
	err := vt.Validate()

	// Assert: Validation MUST succeed
	if err != nil {
		t.Fatalf("expected multi-format table to pass validation, got error: %v", err)
	}
}

// TestTable_AllFormatsValid proves all supported formats are accepted.
//
// Green-Flag: All supported storage formats MUST be valid.
func TestTable_AllFormatsValid(t *testing.T) {
	formats := []tables.StorageFormat{
		tables.FormatDelta,
		tables.FormatIceberg,
		tables.FormatParquet,
	}

	for _, format := range formats {
		t.Run(string(format), func(t *testing.T) {
			// Arrange
			vt := &tables.VirtualTable{
				Name: "test_table",
				Sources: []tables.PhysicalSource{
					{Format: format, Location: "s3://bucket/path"},
				},
				Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
			}

			// Act
			err := vt.Validate()

			// Assert: Validation MUST succeed
			if err != nil {
				t.Fatalf("expected format %s to be valid, got error: %v", format, err)
			}
		})
	}
}

// TestTable_HasCapability proves capability checking works correctly.
//
// Green-Flag: HasCapability MUST return true for present capabilities.
func TestTable_HasCapability(t *testing.T) {
	// Arrange
	vt := &tables.VirtualTable{
		Name: "test_table",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatDelta, Location: "s3://bucket/path"},
		},
		Capabilities: []capabilities.Capability{
			capabilities.CapabilityRead,
			capabilities.CapabilityTimeTravel,
		},
	}

	// Assert
	if !vt.HasCapability(capabilities.CapabilityRead) {
		t.Fatal("expected table to have READ capability")
	}
	if !vt.HasCapability(capabilities.CapabilityTimeTravel) {
		t.Fatal("expected table to have TIME_TRAVEL capability")
	}
}

// TestTable_HasConstraint proves constraint checking works correctly.
//
// Green-Flag: HasConstraint MUST return true for present constraints.
func TestTable_HasConstraint(t *testing.T) {
	// Arrange
	vt := &tables.VirtualTable{
		Name: "test_table",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatDelta, Location: "s3://bucket/path"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
		Constraints: []capabilities.Constraint{
			capabilities.ConstraintReadOnly,
		},
	}

	// Assert
	if !vt.HasConstraint(capabilities.ConstraintReadOnly) {
		t.Fatal("expected table to have READ_ONLY constraint")
	}
	if vt.HasConstraint(capabilities.ConstraintSnapshotConsistent) {
		t.Fatal("expected table to NOT have SNAPSHOT_CONSISTENT constraint")
	}
}
