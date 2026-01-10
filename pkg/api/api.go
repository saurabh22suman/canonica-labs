// Package api defines the public API endpoints and handlers for the canonica gateway.
package api

// API version
const Version = "0.1.0"

// API endpoints
const (
	EndpointQuery       = "/api/v1/query"
	EndpointQueryExplain = "/api/v1/query/explain"
	EndpointQueryValidate = "/api/v1/query/validate"
	EndpointTables      = "/api/v1/tables"
	EndpointEngines     = "/api/v1/engines"
	EndpointAuth        = "/api/v1/auth"
	EndpointHealth      = "/health"
	EndpointReady       = "/ready"
)

// HTTP headers
const (
	HeaderContentType   = "Content-Type"
	HeaderAuthorization = "Authorization"
	HeaderRequestID     = "X-Request-ID"
	HeaderQueryID       = "X-Query-ID"
)

// Content types
const (
	ContentTypeJSON = "application/json"
)
