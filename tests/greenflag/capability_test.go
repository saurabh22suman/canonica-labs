package greenflag

import (
	"testing"

	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/tables"
)

// TestCapability_ReadAllowed proves that SELECT works on tables with READ capability.
//
// Green-Flag: System MUST allow SELECT on tables with READ capability.
func TestCapability_ReadAllowed(t *testing.T) {
	// Arrange: Table with READ capability
	vt := &tables.VirtualTable{
		Name: "readable_table",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatParquet, Location: "s3://bucket/path"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
	}

	// Act
	err := vt.CanPerform(capabilities.OperationSelect)

	// Assert: Operation MUST succeed
	if err != nil {
		t.Fatalf("expected SELECT to be allowed, got error: %v", err)
	}
}

// TestCapability_ParseValidCapabilities proves that valid capabilities parse correctly.
//
// Green-Flag: Valid capability strings MUST parse successfully.
func TestCapability_ParseValidCapabilities(t *testing.T) {
	validCaps := []struct {
		input    string
		expected capabilities.Capability
	}{
		{"READ", capabilities.CapabilityRead},
		{"TIME_TRAVEL", capabilities.CapabilityTimeTravel},
		{"  READ  ", capabilities.CapabilityRead}, // With whitespace
	}

	for _, tc := range validCaps {
		t.Run(tc.input, func(t *testing.T) {
			// Act
			cap, err := capabilities.ParseCapability(tc.input)

			// Assert
			if err != nil {
				t.Fatalf("expected capability %q to parse, got error: %v", tc.input, err)
			}
			if cap != tc.expected {
				t.Fatalf("expected %v, got %v", tc.expected, cap)
			}
		})
	}
}

// TestCapability_ParseValidConstraints proves that valid constraints parse correctly.
//
// Green-Flag: Valid constraint strings MUST parse successfully.
func TestCapability_ParseValidConstraints(t *testing.T) {
	validConstraints := []struct {
		input    string
		expected capabilities.Constraint
	}{
		{"READ_ONLY", capabilities.ConstraintReadOnly},
		{"SNAPSHOT_CONSISTENT", capabilities.ConstraintSnapshotConsistent},
		{"  READ_ONLY  ", capabilities.ConstraintReadOnly}, // With whitespace
	}

	for _, tc := range validConstraints {
		t.Run(tc.input, func(t *testing.T) {
			// Act
			con, err := capabilities.ParseConstraint(tc.input)

			// Assert
			if err != nil {
				t.Fatalf("expected constraint %q to parse, got error: %v", tc.input, err)
			}
			if con != tc.expected {
				t.Fatalf("expected %v, got %v", tc.expected, con)
			}
		})
	}
}

// TestCapability_SetOperations proves that capability sets work correctly.
//
// Green-Flag: CapabilitySet MUST correctly track capabilities.
func TestCapability_SetOperations(t *testing.T) {
	// Arrange
	caps := []capabilities.Capability{
		capabilities.CapabilityRead,
		capabilities.CapabilityTimeTravel,
	}
	set := capabilities.NewCapabilitySet(caps)

	// Assert: Has should return true for included capabilities
	if !set.Has(capabilities.CapabilityRead) {
		t.Fatal("expected set to have READ capability")
	}
	if !set.Has(capabilities.CapabilityTimeTravel) {
		t.Fatal("expected set to have TIME_TRAVEL capability")
	}

	// Assert: Slice should contain all capabilities
	slice := set.Slice()
	if len(slice) != 2 {
		t.Fatalf("expected 2 capabilities in slice, got %d", len(slice))
	}
}
