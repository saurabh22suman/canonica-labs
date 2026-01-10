// Package models provides shared data models for the canonica public API.
package models

import (
	"time"
)

// TableDefinition is the external representation of a virtual table
// used for registration and API responses.
type TableDefinition struct {
	Name         string   `json:"name" yaml:"name"`
	Description  string   `json:"description,omitempty" yaml:"description,omitempty"`
	Sources      []Source `json:"sources" yaml:"sources"`
	Capabilities []string `json:"capabilities" yaml:"capabilities"`
	Constraints  []string `json:"constraints,omitempty" yaml:"constraints,omitempty"`
}

// Source is the external representation of a physical source.
type Source struct {
	Format   string `json:"format" yaml:"format"`
	Location string `json:"location" yaml:"location"`
	Engine   string `json:"engine,omitempty" yaml:"engine,omitempty"`
}

// TableInfo is the API response for table information.
type TableInfo struct {
	Name         string    `json:"name"`
	Description  string    `json:"description,omitempty"`
	Sources      []Source  `json:"sources"`
	Capabilities []string  `json:"capabilities"`
	Constraints  []string  `json:"constraints,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// QueryRequest is the API request for executing a query.
type QueryRequest struct {
	SQL string `json:"sql"`
}

// QueryResponse is the API response for a query execution.
type QueryResponse struct {
	QueryID   string                   `json:"query_id"`
	Columns   []string                 `json:"columns"`
	Rows      []map[string]interface{} `json:"rows"`
	RowCount  int                      `json:"row_count"`
	Engine    string                   `json:"engine"`
	Duration  string                   `json:"duration"`
	Metadata  map[string]string        `json:"metadata,omitempty"`
}

// ExplainResponse is the API response for query explanation.
type ExplainResponse struct {
	SQL                  string   `json:"sql"`
	Operation            string   `json:"operation"`
	Tables               []string `json:"tables"`
	RequiredCapabilities []string `json:"required_capabilities"`
	SelectedEngine       string   `json:"selected_engine"`
	Explanation          string   `json:"explanation"`
}

// ValidationResult is the API response for query validation.
type ValidationResult struct {
	Valid   bool     `json:"valid"`
	SQL     string   `json:"sql"`
	Errors  []string `json:"errors,omitempty"`
}

// EngineInfo is the API response for engine information.
type EngineInfo struct {
	Name         string   `json:"name"`
	Available    bool     `json:"available"`
	Capabilities []string `json:"capabilities"`
	Priority     int      `json:"priority"`
}

// AuthStatus is the API response for authentication status.
type AuthStatus struct {
	Authenticated bool      `json:"authenticated"`
	UserID        string    `json:"user_id,omitempty"`
	UserName      string    `json:"user_name,omitempty"`
	Roles         []string  `json:"roles,omitempty"`
	ExpiresAt     time.Time `json:"expires_at,omitempty"`
}

// ErrorResponse is the API response for errors.
type ErrorResponse struct {
	Error      string `json:"error"`
	Reason     string `json:"reason,omitempty"`
	Suggestion string `json:"suggestion,omitempty"`
	Code       int    `json:"code"`
}
