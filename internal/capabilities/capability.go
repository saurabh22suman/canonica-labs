// Package capabilities defines the capability and constraint model for virtual tables.
// Capabilities represent what operations a table supports.
// Constraints represent restrictions that override or limit capabilities.
//
// Per docs/plan.md: "Users see capabilities (READ, TIME_TRAVEL), not storage formats."
package capabilities

import (
	"fmt"
	"strings"
)

// Capability represents an operation that a virtual table supports.
type Capability string

const (
	// CapabilityRead allows SELECT operations on the table.
	CapabilityRead Capability = "READ"

	// CapabilityTimeTravel allows AS OF queries for point-in-time reads.
	CapabilityTimeTravel Capability = "TIME_TRAVEL"
)

// AllCapabilities returns all valid capabilities.
func AllCapabilities() []Capability {
	return []Capability{
		CapabilityRead,
		CapabilityTimeTravel,
	}
}

// IsValid checks if the capability is a known valid capability.
func (c Capability) IsValid() bool {
	for _, valid := range AllCapabilities() {
		if c == valid {
			return true
		}
	}
	return false
}

// String returns the string representation of the capability.
func (c Capability) String() string {
	return string(c)
}

// ParseCapability parses a string into a Capability.
// Returns an error if the string is not a valid capability.
func ParseCapability(s string) (Capability, error) {
	c := Capability(strings.ToUpper(strings.TrimSpace(s)))
	if !c.IsValid() {
		return "", fmt.Errorf("invalid capability: %s (valid: %v)", s, AllCapabilities())
	}
	return c, nil
}

// Constraint represents a restriction on table operations.
// Constraints override capabilities - a constraint wins over a capability.
type Constraint string

const (
	// ConstraintReadOnly prevents any write operations on the table.
	ConstraintReadOnly Constraint = "READ_ONLY"

	// ConstraintSnapshotConsistent requires all reads to use a consistent snapshot.
	ConstraintSnapshotConsistent Constraint = "SNAPSHOT_CONSISTENT"
)

// AllConstraints returns all valid constraints.
func AllConstraints() []Constraint {
	return []Constraint{
		ConstraintReadOnly,
		ConstraintSnapshotConsistent,
	}
}

// IsValid checks if the constraint is a known valid constraint.
func (c Constraint) IsValid() bool {
	for _, valid := range AllConstraints() {
		if c == valid {
			return true
		}
	}
	return false
}

// String returns the string representation of the constraint.
func (c Constraint) String() string {
	return string(c)
}

// ParseConstraint parses a string into a Constraint.
// Returns an error if the string is not a valid constraint.
func ParseConstraint(s string) (Constraint, error) {
	c := Constraint(strings.ToUpper(strings.TrimSpace(s)))
	if !c.IsValid() {
		return "", fmt.Errorf("invalid constraint: %s (valid: %v)", s, AllConstraints())
	}
	return c, nil
}

// CapabilitySet is a set of capabilities for efficient lookup.
type CapabilitySet map[Capability]struct{}

// NewCapabilitySet creates a new CapabilitySet from a slice of capabilities.
func NewCapabilitySet(caps []Capability) CapabilitySet {
	set := make(CapabilitySet, len(caps))
	for _, c := range caps {
		set[c] = struct{}{}
	}
	return set
}

// Has checks if the set contains the given capability.
func (cs CapabilitySet) Has(c Capability) bool {
	_, ok := cs[c]
	return ok
}

// Add adds a capability to the set.
func (cs CapabilitySet) Add(c Capability) {
	cs[c] = struct{}{}
}

// Slice returns the capabilities as a slice.
func (cs CapabilitySet) Slice() []Capability {
	result := make([]Capability, 0, len(cs))
	for c := range cs {
		result = append(result, c)
	}
	return result
}

// ConstraintSet is a set of constraints for efficient lookup.
type ConstraintSet map[Constraint]struct{}

// NewConstraintSet creates a new ConstraintSet from a slice of constraints.
func NewConstraintSet(constraints []Constraint) ConstraintSet {
	set := make(ConstraintSet, len(constraints))
	for _, c := range constraints {
		set[c] = struct{}{}
	}
	return set
}

// Has checks if the set contains the given constraint.
func (cs ConstraintSet) Has(c Constraint) bool {
	_, ok := cs[c]
	return ok
}

// Add adds a constraint to the set.
func (cs ConstraintSet) Add(c Constraint) {
	cs[c] = struct{}{}
}

// Slice returns the constraints as a slice.
func (cs ConstraintSet) Slice() []Constraint {
	result := make([]Constraint, 0, len(cs))
	for c := range cs {
		result = append(result, c)
	}
	return result
}

// OperationType represents the type of SQL operation.
type OperationType string

const (
	OperationSelect OperationType = "SELECT"
	OperationInsert OperationType = "INSERT"
	OperationUpdate OperationType = "UPDATE"
	OperationDelete OperationType = "DELETE"
)

// RequiredCapability returns the capability required for an operation type.
func (op OperationType) RequiredCapability() Capability {
	switch op {
	case OperationSelect:
		return CapabilityRead
	default:
		// Writes not supported in MVP
		return ""
	}
}

// IsWriteOperation returns true if the operation modifies data.
func (op OperationType) IsWriteOperation() bool {
	switch op {
	case OperationInsert, OperationUpdate, OperationDelete:
		return true
	default:
		return false
	}
}
