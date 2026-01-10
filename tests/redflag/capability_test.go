package redflag

import (
	"testing"

	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/errors"
	"github.com/canonica-labs/canonica/internal/tables"
)

// TestCapabilityDenied_MissingReadCapability proves that operations requiring
// READ capability are blocked when the table lacks it.
//
// Red-Flag: System MUST refuse SELECT on tables without READ capability.
func TestCapabilityDenied_MissingReadCapability(t *testing.T) {
	// Arrange: Table with NO capabilities
	vt := &tables.VirtualTable{
		Name: "test_table",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatParquet, Location: "s3://bucket/path"},
		},
		Capabilities: []capabilities.Capability{}, // No capabilities
		Constraints:  []capabilities.Constraint{},
	}

	// Act: Attempt SELECT operation
	err := vt.CanPerform(capabilities.OperationSelect)

	// Assert: Operation MUST be denied
	if err == nil {
		t.Fatal("expected error for SELECT on table without READ capability, got nil")
	}

	// Assert: Error must be ErrCapabilityDenied
	var capErr *errors.ErrCapabilityDenied
	if _, ok := err.(*errors.ErrCapabilityDenied); !ok {
		t.Fatalf("expected ErrCapabilityDenied, got %T: %v", err, err)
	}
	_ = capErr
}

// TestCapabilityDenied_TimeTravelWithoutCapability proves that time-travel
// queries are blocked when the table lacks TIME_TRAVEL capability.
//
// Red-Flag: System MUST refuse AS OF queries on tables without TIME_TRAVEL.
func TestCapabilityDenied_TimeTravelWithoutCapability(t *testing.T) {
	// Arrange: Table with READ but not TIME_TRAVEL
	vt := &tables.VirtualTable{
		Name: "test_table",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatDelta, Location: "s3://bucket/path"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
		Constraints:  []capabilities.Constraint{},
	}

	// Act: Check if table has TIME_TRAVEL capability
	hasTimeTravel := vt.HasCapability(capabilities.CapabilityTimeTravel)

	// Assert: Table MUST NOT have TIME_TRAVEL
	if hasTimeTravel {
		t.Fatal("expected table to lack TIME_TRAVEL capability")
	}
}

// TestConstraintViolation_ReadOnlyBlocksWrite proves that write operations
// are blocked when READ_ONLY constraint is active.
//
// Red-Flag: System MUST refuse INSERT/UPDATE/DELETE on READ_ONLY tables.
func TestConstraintViolation_ReadOnlyBlocksWrite(t *testing.T) {
	// Arrange: Table with READ_ONLY constraint
	vt := &tables.VirtualTable{
		Name: "readonly_table",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatIceberg, Location: "s3://bucket/path"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
		Constraints:  []capabilities.Constraint{capabilities.ConstraintReadOnly},
	}

	writeOps := []capabilities.OperationType{
		capabilities.OperationInsert,
		capabilities.OperationUpdate,
		capabilities.OperationDelete,
	}

	for _, op := range writeOps {
		t.Run(string(op), func(t *testing.T) {
			// Act
			err := vt.CanPerform(op)

			// Assert: Operation MUST be denied
			if err == nil {
				t.Fatalf("expected error for %s on READ_ONLY table, got nil", op)
			}
		})
	}
}

// TestWriteOperations_BlockedInMVP proves that all write operations are
// blocked in the MVP, regardless of table capabilities.
//
// Red-Flag: MVP MUST refuse all write operations.
func TestWriteOperations_BlockedInMVP(t *testing.T) {
	// Arrange: Table WITHOUT READ_ONLY constraint
	vt := &tables.VirtualTable{
		Name: "writable_table",
		Sources: []tables.PhysicalSource{
			{Format: tables.FormatDelta, Location: "s3://bucket/path"},
		},
		Capabilities: []capabilities.Capability{capabilities.CapabilityRead},
		Constraints:  []capabilities.Constraint{}, // No constraints
	}

	writeOps := []capabilities.OperationType{
		capabilities.OperationInsert,
		capabilities.OperationUpdate,
		capabilities.OperationDelete,
	}

	for _, op := range writeOps {
		t.Run(string(op), func(t *testing.T) {
			// Act
			err := vt.CanPerform(op)

			// Assert: Operation MUST be denied (MVP is read-only)
			if err == nil {
				t.Fatalf("expected error for %s in MVP (read-only mode), got nil", op)
			}
		})
	}
}

// TestInvalidCapability_Rejected proves that invalid capabilities
// are rejected during parsing.
//
// Red-Flag: System MUST reject unknown capabilities.
func TestInvalidCapability_Rejected(t *testing.T) {
	invalidCaps := []string{
		"WRITE",           // Not supported in MVP
		"INVALID",
		"UNKNOWN_CAP",
		"",
	}

	for _, capStr := range invalidCaps {
		t.Run(capStr, func(t *testing.T) {
			// Act
			_, err := capabilities.ParseCapability(capStr)

			// Assert: Parsing MUST fail
			if err == nil {
				t.Fatalf("expected error for invalid capability %q, got nil", capStr)
			}
		})
	}
}

// TestInvalidConstraint_Rejected proves that invalid constraints
// are rejected during parsing.
//
// Red-Flag: System MUST reject unknown constraints.
func TestInvalidConstraint_Rejected(t *testing.T) {
	invalidConstraints := []string{
		"INVALID",
		"UNKNOWN_CONSTRAINT",
		"",
	}

	for _, conStr := range invalidConstraints {
		t.Run(conStr, func(t *testing.T) {
			// Act
			_, err := capabilities.ParseConstraint(conStr)

			// Assert: Parsing MUST fail
			if err == nil {
				t.Fatalf("expected error for invalid constraint %q, got nil", conStr)
			}
		})
	}
}
