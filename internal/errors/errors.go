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

// ErrAccessDenied is returned when authorization fails (user lacks permission).
// Per phase-2-spec.md §4: "clearly state the unauthorized table" and "identify the missing capability".
type ErrAccessDenied struct {
	CanonicError
	Table      string
	Capability string
}

// NewAccessDenied creates a new ErrAccessDenied.
// Per phase-2-spec.md: Error messages must clearly state the unauthorized table
// and identify the missing capability.
func NewAccessDenied(table, capability, reason string) *ErrAccessDenied {
	msg := "access denied"
	if table != "" {
		msg = fmt.Sprintf("access denied on table '%s'", table)
	}
	suggestion := "contact administrator for access"
	if table != "" {
		suggestion = fmt.Sprintf("request %s permission on '%s' from administrator", capability, table)
	}
	return &ErrAccessDenied{
		CanonicError: CanonicError{
			Code:       CodeAuth,
			Message:    msg,
			Reason:     reason,
			Suggestion: suggestion,
		},
		Table:      table,
		Capability: capability,
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

// ErrWriteNotAllowed is returned when a write operation is attempted in read-only mode.
type ErrWriteNotAllowed struct {
	CanonicError
	Operation string
}

// NewWriteNotAllowed creates an error for write operations in read-only mode.
func NewWriteNotAllowed(operation string) *ErrWriteNotAllowed {
	return &ErrWriteNotAllowed{
		CanonicError: CanonicError{
			Code:       CodeValidation,
			Message:    fmt.Sprintf("%s operation not allowed", operation),
			Reason:     "system is in read-only mode (MVP)",
			Suggestion: "only SELECT queries are supported",
		},
		Operation: operation,
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

// ErrTableAlreadyExists is returned when trying to create a table that already exists.
type ErrTableAlreadyExists struct {
	CanonicError
	Table string
}

// NewTableAlreadyExists creates a new ErrTableAlreadyExists.
func NewTableAlreadyExists(table string) *ErrTableAlreadyExists {
	return &ErrTableAlreadyExists{
		CanonicError: CanonicError{
			Code:       CodeValidation,
			Message:    fmt.Sprintf("table already exists: %s", table),
			Reason:     "a table with this name is already registered",
			Suggestion: "use a different name or update the existing table",
		},
		Table: table,
	}
}

// ErrUnsupportedSyntax is returned when a query uses unsupported SQL syntax.
// Per phase-3-spec.md §9: "Parser rejections must be explicit, stable, and human-readable."
type ErrUnsupportedSyntax struct {
	CanonicError
	Construct   string // The unsupported SQL construct (e.g., "WINDOW FUNCTION", "CTE")
	Alternative string // A supported alternative, if available
}

// NewUnsupportedSyntax creates an error for unsupported SQL syntax.
// Per phase-3-spec.md §9: Error messages MUST include:
// - unsupported construct
// - example of supported alternative (when possible)
func NewUnsupportedSyntax(construct, alternative string) *ErrUnsupportedSyntax {
	suggestion := "only simple SELECT queries are supported in MVP"
	if alternative != "" {
		suggestion = fmt.Sprintf("Supported: %s", alternative)
	}
	return &ErrUnsupportedSyntax{
		CanonicError: CanonicError{
			Code:       CodeValidation,
			Message:    fmt.Sprintf("Unsupported SQL construct: %s", construct),
			Reason:     fmt.Sprintf("%s is not supported in canonica MVP", construct),
			Suggestion: suggestion,
		},
		Construct:   construct,
		Alternative: alternative,
	}
}

// ErrVendorHint is returned when a query contains vendor-specific hints.
// Per phase-3-spec.md §9: Vendor-specific hints must fail with specific error.
type ErrVendorHint struct {
	CanonicError
	HintType string
}

// NewVendorHint creates an error for vendor-specific hints.
func NewVendorHint(hintType string) *ErrVendorHint {
	return &ErrVendorHint{
		CanonicError: CanonicError{
			Code:       CodeValidation,
			Message:    fmt.Sprintf("Unsupported SQL construct: VENDOR HINT (%s)", hintType),
			Reason:     "vendor-specific hints are not supported",
			Suggestion: "remove the hint and use standard SQL",
		},
		HintType: hintType,
	}
}

// ErrGatewayUnavailable is returned when the CLI cannot connect to the gateway.
// Per phase-3-spec.md §8: "If the gateway is unreachable, the CLI MUST fail."
type ErrGatewayUnavailable struct {
	CanonicError
	Endpoint string
}

// NewGatewayUnavailable creates an error for gateway connectivity failure.
func NewGatewayUnavailable(endpoint, reason string) *ErrGatewayUnavailable {
	return &ErrGatewayUnavailable{
		CanonicError: CanonicError{
			Code:       CodeInternal,
			Message:    "gateway unavailable",
			Reason:     reason,
			Suggestion: fmt.Sprintf("check gateway is running at %s", endpoint),
		},
		Endpoint: endpoint,
	}
}

// ErrDatabaseUnavailable is returned when the gateway cannot connect to PostgreSQL.
// Per phase-3-spec.md §7: "Gateway starts without PostgreSQL → must fail"
type ErrDatabaseUnavailable struct {
	CanonicError
}

// NewDatabaseUnavailable creates an error for database connectivity failure.
func NewDatabaseUnavailable(reason string) *ErrDatabaseUnavailable {
	return &ErrDatabaseUnavailable{
		CanonicError: CanonicError{
			Code:       CodeInternal,
			Message:    "database unavailable",
			Reason:     reason,
			Suggestion: "check PostgreSQL is running and connection configuration is correct",
		},
	}
}

// ErrMetadataConflict is returned when conflicting metadata sources are detected.
// Per phase-3-spec.md §7: "Two conflicting metadata sources detected → must fail"
type ErrMetadataConflict struct {
	CanonicError
	Resource string
	Source1  string
	Source2  string
}

// NewMetadataConflict creates an error for conflicting metadata sources.
func NewMetadataConflict(resource, source1, source2 string) *ErrMetadataConflict {
	return &ErrMetadataConflict{
		CanonicError: CanonicError{
			Code:       CodeInternal,
			Message:    "metadata conflict detected",
			Reason:     fmt.Sprintf("conflicting definitions for '%s' from '%s' and '%s'", resource, source1, source2),
			Suggestion: "ensure only PostgreSQL is the metadata authority",
		},
		Resource: resource,
		Source1:  source1,
		Source2:  source2,
	}
}

// ErrBootstrapError is returned when bootstrap operations fail.
// Per phase-5-spec.md §2: "Errors must explain what failed, why, and how to fix"
type ErrBootstrapError struct {
	CanonicError
}

// NewBootstrapError creates an error for bootstrap operation failures.
func NewBootstrapError(message, reason, suggestion string) *ErrBootstrapError {
	return &ErrBootstrapError{
		CanonicError: CanonicError{
			Code:       CodeValidation,
			Message:    message,
			Reason:     reason,
			Suggestion: suggestion,
		},
	}
}

// ErrMigrationFailed is returned when a database migration fails.
// Per execution-checklist.md 4.4: Gateway fails startup on migration failure.
type ErrMigrationFailed struct {
	CanonicError
	Migration string
}

// NewMigrationFailed creates an error for migration failures.
func NewMigrationFailed(migration string, cause error) *ErrMigrationFailed {
	return &ErrMigrationFailed{
		CanonicError: CanonicError{
			Code:       CodeInternal,
			Message:    fmt.Sprintf("migration failed: %s", migration),
			Reason:     cause.Error(),
			Suggestion: "check database connection and migration file syntax",
			Cause:      cause,
		},
		Migration: migration,
	}
}

// ErrPlannerError is returned when query planning fails.
// Per phase-8-spec.md: Planner errors are explicit and actionable.
type ErrPlannerError struct {
	CanonicError
}

// NewPlannerError creates an error for planner failures.
func NewPlannerError(reason string) *ErrPlannerError {
	return &ErrPlannerError{
		CanonicError: CanonicError{
			Code:       CodeValidation,
			Message:    "query planning failed",
			Reason:     reason,
			Suggestion: "check query syntax and ensure all referenced tables exist",
		},
	}
}

// ErrCrossEngineQuery is returned when a query spans multiple engines.
// Per phase-9-spec.md: Cross-engine queries require federation.
type ErrCrossEngineQuery struct {
	CanonicError
	Engines []string
}

// NewCrossEngineQuery creates an error for cross-engine queries.
// This error signals that federation is needed.
func NewCrossEngineQuery(engines []string) *ErrCrossEngineQuery {
	return &ErrCrossEngineQuery{
		CanonicError: CanonicError{
			Code:       CodeValidation,
			Message:    "query spans multiple engines",
			Reason:     fmt.Sprintf("query references tables on engines: %v", engines),
			Suggestion: "enable federation or ensure all tables use the same engine",
		},
		Engines: engines,
	}
}
