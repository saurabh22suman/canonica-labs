// Package tables provides the virtual table abstraction layer.
// Virtual tables abstract physical storage and expose capabilities/constraints.
//
// Per docs/plan.md: "Users see unified virtual tables, not underlying Delta/Iceberg/Hudi."
package tables

import (
	"fmt"
	"time"

	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/errors"
)

// VirtualTable represents a logical table abstraction over physical storage.
// This is the core model of the Table Abstraction Layer.
type VirtualTable struct {
	// Name is the unique identifier for this virtual table.
	Name string `json:"name"`

	// Description is a human-readable description of the table.
	Description string `json:"description,omitempty"`

	// Sources are the physical storage locations backing this table.
	Sources []PhysicalSource `json:"sources"`

	// Capabilities are the operations this table supports.
	Capabilities []capabilities.Capability `json:"capabilities"`

	// Constraints are restrictions on table operations.
	Constraints []capabilities.Constraint `json:"constraints"`

	// CreatedAt is when the table was registered.
	CreatedAt time.Time `json:"created_at"`

	// UpdatedAt is when the table was last modified.
	UpdatedAt time.Time `json:"updated_at"`

	// cachedCapabilitySet is populated on first access for efficient lookups.
	cachedCapabilitySet capabilities.CapabilitySet

	// cachedConstraintSet is populated on first access for efficient lookups.
	cachedConstraintSet capabilities.ConstraintSet
}

// PhysicalSource represents the physical storage backing a virtual table.
type PhysicalSource struct {
	// Format is the storage format (e.g., DELTA, ICEBERG, PARQUET).
	Format StorageFormat `json:"format"`

	// Location is the URI of the physical storage (e.g., s3://bucket/path).
	Location string `json:"location"`

	// Engine is the preferred engine for this source (optional).
	Engine string `json:"engine,omitempty"`
}

// StorageFormat represents the physical storage format.
type StorageFormat string

const (
	FormatDelta   StorageFormat = "DELTA"
	FormatIceberg StorageFormat = "ICEBERG"
	FormatParquet StorageFormat = "PARQUET"
)

// AllFormats returns all valid storage formats.
func AllFormats() []StorageFormat {
	return []StorageFormat{FormatDelta, FormatIceberg, FormatParquet}
}

// IsValid checks if the format is a known valid format.
func (f StorageFormat) IsValid() bool {
	for _, valid := range AllFormats() {
		if f == valid {
			return true
		}
	}
	return false
}

// CapabilitySet returns the capability set for efficient lookups.
func (vt *VirtualTable) CapabilitySet() capabilities.CapabilitySet {
	if vt.cachedCapabilitySet == nil {
		vt.cachedCapabilitySet = capabilities.NewCapabilitySet(vt.Capabilities)
	}
	return vt.cachedCapabilitySet
}

// ConstraintSet returns the constraint set for efficient lookups.
func (vt *VirtualTable) ConstraintSet() capabilities.ConstraintSet {
	if vt.cachedConstraintSet == nil {
		vt.cachedConstraintSet = capabilities.NewConstraintSet(vt.Constraints)
	}
	return vt.cachedConstraintSet
}

// HasCapability checks if the table has the given capability.
func (vt *VirtualTable) HasCapability(cap capabilities.Capability) bool {
	return vt.CapabilitySet().Has(cap)
}

// HasConstraint checks if the table has the given constraint.
func (vt *VirtualTable) HasConstraint(con capabilities.Constraint) bool {
	return vt.ConstraintSet().Has(con)
}

// CanPerform checks if an operation can be performed on this table.
// Returns nil if allowed, or an error explaining why it's forbidden.
func (vt *VirtualTable) CanPerform(op capabilities.OperationType) error {
	// Check if operation is a write (blocked in MVP)
	if op.IsWriteOperation() {
		// Check READ_ONLY constraint
		if vt.HasConstraint(capabilities.ConstraintReadOnly) {
			return errors.NewConstraintViolation(vt.Name, string(capabilities.ConstraintReadOnly), string(op))
		}
		// Even without READ_ONLY, writes are not supported in MVP
		return errors.NewWriteNotAllowed(string(op))
	}

	// Check required capability
	requiredCap := op.RequiredCapability()
	if requiredCap != "" && !vt.HasCapability(requiredCap) {
		return errors.NewCapabilityDenied(vt.Name, string(requiredCap), string(op))
	}

	return nil
}

// Validate checks if the virtual table definition is valid.
// Returns nil if valid, or an error describing the problem.
func (vt *VirtualTable) Validate() error {
	// Name is required
	if vt.Name == "" {
		return errors.NewInvalidTableDefinition("name", "required")
	}

	// At least one source is required
	if len(vt.Sources) == 0 {
		return errors.NewInvalidTableDefinition("sources", "at least one source required")
	}

	// Validate each source
	for i, src := range vt.Sources {
		if src.Location == "" {
			return errors.NewInvalidTableDefinition(
				fmt.Sprintf("sources[%d].location", i),
				"required",
			)
		}
		if !src.Format.IsValid() {
			return errors.NewInvalidTableDefinition(
				fmt.Sprintf("sources[%d].format", i),
				fmt.Sprintf("invalid format: %s (valid: %v)", src.Format, AllFormats()),
			)
		}
	}

	// Validate capabilities
	for i, cap := range vt.Capabilities {
		if !cap.IsValid() {
			return errors.NewInvalidTableDefinition(
				fmt.Sprintf("capabilities[%d]", i),
				fmt.Sprintf("invalid capability: %s", cap),
			)
		}
	}

	// Validate constraints
	for i, con := range vt.Constraints {
		if !con.IsValid() {
			return errors.NewInvalidTableDefinition(
				fmt.Sprintf("constraints[%d]", i),
				fmt.Sprintf("invalid constraint: %s", con),
			)
		}
	}

	// Check for conflicting sources (same format, different locations)
	// This would create ambiguity in which source to use
	formatLocations := make(map[StorageFormat]string)
	for _, src := range vt.Sources {
		if existing, ok := formatLocations[src.Format]; ok && existing != src.Location {
			return errors.NewInvalidTableDefinition(
				"sources",
				fmt.Sprintf("conflicting sources for format %s", src.Format),
			)
		}
		formatLocations[src.Format] = src.Location
	}

	return nil
}
