// Package errors provides explicit, human-readable error types for canonica.
// All errors must include a Reason and Suggestion for actionable feedback.
//
// Per docs/plan.md: "Errors must be understandable. If you can't explain the failure, don't ship."
package errors

import (
	"fmt"
)

// CanonicError is the base error type for all canonica errors.
// Every error must provide a human-readable reason and suggestion.
type CanonicError struct {
	Code       ErrorCode
	Message    string
	Reason     string
	Suggestion string
	Cause      error
}

// ErrorCode represents the category of error for exit code mapping.
type ErrorCode int

const (
	CodeValidation ErrorCode = 1
	CodeAuth       ErrorCode = 2
	CodeEngine     ErrorCode = 3
	CodeInternal   ErrorCode = 4
)

func (e *CanonicError) Error() string {
	msg := e.Message
	if e.Reason != "" {
		msg = fmt.Sprintf("%s\nReason: %s", msg, e.Reason)
	}
	if e.Suggestion != "" {
		msg = fmt.Sprintf("%s\nSuggestion: %s", msg, e.Suggestion)
	}
	if e.Cause != nil {
		msg = fmt.Sprintf("%s\nCaused by: %v", msg, e.Cause)
	}
	return msg
}

func (e *CanonicError) Unwrap() error {
	return e.Cause
}

// ErrCapabilityDenied is returned when an operation requires a capability
// that the virtual table does not have.
type ErrCapabilityDenied struct {
	CanonicError
	Table      string
	Capability string
	Operation  string
}

// NewCapabilityDenied creates a new ErrCapabilityDenied.
func NewCapabilityDenied(table, capability, operation string) *ErrCapabilityDenied {
	return &ErrCapabilityDenied{
		CanonicError: CanonicError{
			Code:       CodeValidation,
			Message:    fmt.Sprintf("%s forbidden on %s", operation, table),
			Reason:     fmt.Sprintf("table lacks %s capability", capability),
			Suggestion: fmt.Sprintf("check table capabilities with 'canonic table describe %s'", table),
		},
		Table:      table,
		Capability: capability,
		Operation:  operation,
	}
}

// ErrConstraintViolation is returned when an operation violates a table constraint.
type ErrConstraintViolation struct {
	CanonicError
	Table      string
	Constraint string
	Operation  string
}

// NewConstraintViolation creates a new ErrConstraintViolation.
func NewConstraintViolation(table, constraint, operation string) *ErrConstraintViolation {
	return &ErrConstraintViolation{
		CanonicError: CanonicError{
			Code:       CodeValidation,
			Message:    fmt.Sprintf("%s forbidden on %s", operation, table),
			Reason:     fmt.Sprintf("%s constraint active", constraint),
			Suggestion: fmt.Sprintf("check table constraints with 'canonic table describe %s'", table),
		},
		Table:      table,
		Constraint: constraint,
		Operation:  operation,
	}
}

// ErrTableNotFound is returned when a referenced table does not exist.
type ErrTableNotFound struct {
	CanonicError
	Table string
}

// NewTableNotFound creates a new ErrTableNotFound.
func NewTableNotFound(table string) *ErrTableNotFound {
	return &ErrTableNotFound{
		CanonicError: CanonicError{
			Code:       CodeValidation,
			Message:    fmt.Sprintf("table not found: %s", table),
			Reason:     "no virtual table registered with this name",
			Suggestion: "list available tables with 'canonic table list'",
		},
		Table: table,
	}
}

// ErrEngineUnavailable is returned when no compatible engine is available for a query.
type ErrEngineUnavailable struct {
	CanonicError
	RequiredCapabilities []string
}

// NewEngineUnavailable creates a new ErrEngineUnavailable.
func NewEngineUnavailable(required []string) *ErrEngineUnavailable {
	return &ErrEngineUnavailable{
		CanonicError: CanonicError{
			Code:       CodeEngine,
			Message:    "no compatible engine available",
			Reason:     fmt.Sprintf("query requires capabilities: %v", required),
			Suggestion: "check engine status with 'canonic engine list'",
		},
		RequiredCapabilities: required,
	}
}

// ErrAuthFailed is returned when authentication fails.
type ErrAuthFailed struct {
	CanonicError
}

// NewAuthFailed creates a new ErrAuthFailed.
func NewAuthFailed(reason string) *ErrAuthFailed {
	return &ErrAuthFailed{
		CanonicError: CanonicError{
			Code:       CodeAuth,
			Message:    "authentication failed",
			Reason:     reason,
			Suggestion: "authenticate with 'canonic auth login'",
		},
	}
}

// ErrAuthExpired is returned when the auth token has expired.
func NewAuthExpired() *ErrAuthFailed {
	return &ErrAuthFailed{
		CanonicError: CanonicError{
			Code:       CodeAuth,
			Message:    "authentication expired",
			Reason:     "token has expired",
			Suggestion: "re-authenticate with 'canonic auth login'",
		},
	}
}

// ErrQueryRejected is returned when a query is rejected before execution.
type ErrQueryRejected struct {
	CanonicError
	Query string
}

// NewQueryRejected creates a new ErrQueryRejected.
func NewQueryRejected(query, reason, suggestion string) *ErrQueryRejected {
	return &ErrQueryRejected{
		CanonicError: CanonicError{
			Code:       CodeValidation,
			Message:    "query rejected",
			Reason:     reason,
			Suggestion: suggestion,
		},
		Query: query,
	}
}

// NewWriteNotAllowed creates an error for write operations in read-only mode.
func NewWriteNotAllowed(operation string) *ErrQueryRejected {
	return &ErrQueryRejected{
		CanonicError: CanonicError{
			Code:       CodeValidation,
			Message:    fmt.Sprintf("%s operation not allowed", operation),
			Reason:     "system is in read-only mode (MVP)",
			Suggestion: "only SELECT queries are supported",
		},
	}
}

// ErrAmbiguousTable is returned when table resolution is ambiguous.
type ErrAmbiguousTable struct {
	CanonicError
	Table    string
	Matches  []string
}

// NewAmbiguousTable creates a new ErrAmbiguousTable.
func NewAmbiguousTable(table string, matches []string) *ErrAmbiguousTable {
	return &ErrAmbiguousTable{
		CanonicError: CanonicError{
			Code:       CodeValidation,
			Message:    fmt.Sprintf("ambiguous table reference: %s", table),
			Reason:     fmt.Sprintf("multiple tables match: %v", matches),
			Suggestion: "use fully qualified table name",
		},
		Table:   table,
		Matches: matches,
	}
}

// ErrInvalidTableDefinition is returned when a table registration is invalid.
type ErrInvalidTableDefinition struct {
	CanonicError
	Field string
}

// NewInvalidTableDefinition creates a new ErrInvalidTableDefinition.
func NewInvalidTableDefinition(field, reason string) *ErrInvalidTableDefinition {
	return &ErrInvalidTableDefinition{
		CanonicError: CanonicError{
			Code:       CodeValidation,
			Message:    "invalid table definition",
			Reason:     fmt.Sprintf("field '%s': %s", field, reason),
			Suggestion: "check table schema in docs/canonic-cli-spec.md",
		},
		Field: field,
	}
}
