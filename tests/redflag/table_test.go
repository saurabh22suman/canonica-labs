package redflag

import (
	"testing"

	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/errors"
	"github.com/canonica-labs/canonica/internal/tables"
)

// TestTableValidation_MissingName proves that tables without names are rejected.
//
// Red-Flag: System MUST reject table registration without a name.
func TestTableValidation_MissingName(t *testing.T) {
	// Arrange: Table with no name
	vt := &tables.VirtualTable{
		Name: "", // Missing name
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatParquet, Location: "s3://bucket/path"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	}

	// Act
	err := vt.Validate()

	// Assert: Validation MUST fail
	if err == nil {
		t.Fatal("expected error for table without name, got nil")
	}

	// Assert: Error must be ErrInvalidTableDefinition
	if _, ok := err.(*errors.ErrInvalidTableDefinition); !ok {
		t.Fatalf("expected ErrInvalidTableDefinition, got %T: %v", err, err)
	}
}

// TestTableValidation_MissingSources proves that tables without sources are rejected.
//
// Red-Flag: System MUST reject table registration without physical sources.
func TestTableValidation_MissingSources(t *testing.T) {
	// Arrange: Table with no sources
	vt := &tables.VirtualTable{
		Name:         "test_table",
		Sources:      []tables.PhysicalSource{}, // No sources
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	}

	// Act
	err := vt.Validate()

	// Assert: Validation MUST fail
	if err == nil {
		t.Fatal("expected error for table without sources, got nil")
	}
}

// TestTableValidation_MissingLocation proves that sources without locations are rejected.
//
// Red-Flag: System MUST reject sources without location.
func TestTableValidation_MissingLocation(t *testing.T) {
	// Arrange: Source with no location
	vt := &tables.VirtualTable{
		Name: "test_table",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatParquet, Location: ""}, // Missing location
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	}

	// Act
	err := vt.Validate()

	// Assert: Validation MUST fail
	if err == nil {
		t.Fatal("expected error for source without location, got nil")
	}
}

// TestTableValidation_InvalidFormat proves that unknown storage formats are rejected.
//
// Red-Flag: System MUST reject unknown storage formats.
func TestTableValidation_InvalidFormat(t *testing.T) {
	// Arrange: Source with invalid format
	vt := &tables.VirtualTable{
		Name: "test_table",
		Sources: []tables.PhysicalSource{
			{Format: tables.StorageFormat("UNKNOWN"), Location: "s3://bucket/path"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	}

	// Act
	err := vt.Validate()

	// Assert: Validation MUST fail
	if err == nil {
		t.Fatal("expected error for invalid storage format, got nil")
	}
}

// TestTableValidation_ConflictingSources proves that conflicting sources are rejected.
//
// Red-Flag: System MUST reject tables with conflicting sources for the same format.
func TestTableValidation_ConflictingSources(t *testing.T) {
	// Arrange: Two sources with same format but different locations
	vt := &tables.VirtualTable{
		Name: "test_table",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatDelta, Location: "s3://bucket/path1"},
			{Format: tables.FormatDelta, Location: "s3://bucket/path2"}, // Conflict!
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	}

	// Act
	err := vt.Validate()

	// Assert: Validation MUST fail
	if err == nil {
		t.Fatal("expected error for conflicting sources, got nil")
	}
}

// TestTableValidation_InvalidCapability proves that tables with invalid capabilities are rejected.
//
// Red-Flag: System MUST reject tables with unknown capabilities.
func TestTableValidation_InvalidCapability(t *testing.T) {
	// Arrange: Table with invalid capability
	vt := &tables.VirtualTable{
		Name: "test_table",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatParquet, Location: "s3://bucket/path"},
		},
		Capabilities: []capabilities.Capability{capabilities.Capability("INVALID")},
	}

	// Act
	err := vt.Validate()

	// Assert: Validation MUST fail
	if err == nil {
		t.Fatal("expected error for invalid capability, got nil")
	}
}

// TestTableValidation_InvalidConstraint proves that tables with invalid constraints are rejected.
//
// Red-Flag: System MUST reject tables with unknown constraints.
func TestTableValidation_InvalidConstraint(t *testing.T) {
	// Arrange: Table with invalid constraint
	vt := &tables.VirtualTable{
		Name: "test_table",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatParquet, Location: "s3://bucket/path"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
		Constraints:  []capabilities.Constraint{capabilities.Constraint("INVALID")},
	}

	// Act
	err := vt.Validate()

	// Assert: Validation MUST fail
	if err == nil {
		t.Fatal("expected error for invalid constraint, got nil")
	}
}
