// Package router provides engine selection and query routing.
// The router is deterministic and rule-based - no cost estimation or ML.
//
// Per docs/plan.md: "Engine selection is rule-based, deterministic, and explainable."
package router

import (
	"context"
	"sync"

	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/errors"
)

// Engine represents a query engine that can execute SQL.
type Engine struct {
	// Name is the unique identifier for this engine.
	Name string

	// Capabilities are the operations this engine supports.
	Capabilities []capabilities.Capability

	// Available indicates if the engine is currently available.
	Available bool

	// Priority is used for engine selection when multiple engines qualify.
	// Lower numbers = higher priority.
	Priority int
}

// HasCapability checks if the engine has the given capability.
func (e *Engine) HasCapability(cap capabilities.Capability) bool {
	for _, c := range e.Capabilities {
		if c == cap {
			return true
		}
	}
	return false
}

// HasAllCapabilities checks if the engine has all the given capabilities.
func (e *Engine) HasAllCapabilities(caps []capabilities.Capability) bool {
	for _, required := range caps {
		if !e.HasCapability(required) {
			return false
		}
	}
	return true
}

// Router selects engines for query execution.
type Router struct {
	mu      sync.RWMutex
	engines map[string]*Engine
}

// NewRouter creates a new router.
func NewRouter() *Router {
	return &Router{
		engines: make(map[string]*Engine),
	}
}

// RegisterEngine adds an engine to the router.
func (r *Router) RegisterEngine(engine *Engine) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.engines[engine.Name] = engine
}

// SelectEngine selects the best available engine for the given capabilities.
// Returns the engine name or an error if no engine is available.
func (r *Router) SelectEngine(ctx context.Context, required []capabilities.Capability) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var bestEngine *Engine = nil

	for _, engine := range r.engines {
		// Skip unavailable engines
		if !engine.Available {
			continue
		}

		// Check if engine has all required capabilities
		if !engine.HasAllCapabilities(required) {
			continue
		}

		// Select this engine if it's the first match or has higher priority
		if bestEngine == nil || engine.Priority < bestEngine.Priority {
			bestEngine = engine
		}
	}

	if bestEngine == nil {
		capStrings := make([]string, len(required))
		for i, c := range required {
			capStrings[i] = string(c)
		}
		return "", errors.NewEngineUnavailable(capStrings)
	}

	return bestEngine.Name, nil
}

// AvailableEngines returns the list of available engine names.
func (r *Router) AvailableEngines(ctx context.Context) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]string, 0, len(r.engines))
	for name, engine := range r.engines {
		if engine.Available {
			result = append(result, name)
		}
	}
	return result
}

// GetEngine returns an engine by name.
func (r *Router) GetEngine(name string) (*Engine, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	engine, ok := r.engines[name]
	return engine, ok
}

// SetEngineAvailability updates the availability of an engine.
func (r *Router) SetEngineAvailability(name string, available bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if engine, ok := r.engines[name]; ok {
		engine.Available = available
	}
}

// DefaultRouter creates a router with the default MVP engines.
// MVP uses DuckDB only. See tracker.md T002, T003 for Trino/Spark.
func DefaultRouter() *Router {
	router := NewRouter()

	// DuckDB - local engine for MVP and dev
	router.RegisterEngine(&Engine{
		Name: "duckdb",
		Capabilities: []capabilities.Capability{
			capabilities.CapabilityRead,
			// DuckDB supports time travel for Delta/Iceberg via extensions
			capabilities.CapabilityTimeTravel,
		},
		Available: true,
		Priority:  1, // Primary for MVP
	})

	// Trino - placeholder for future (tracker.md T002)
	router.RegisterEngine(&Engine{
		Name: "trino",
		Capabilities: []capabilities.Capability{
			capabilities.CapabilityRead,
			capabilities.CapabilityTimeTravel,
		},
		Available: false, // Not implemented yet
		Priority:  2,
	})

	// Spark - placeholder for future (tracker.md T003)
	router.RegisterEngine(&Engine{
		Name: "spark",
		Capabilities: []capabilities.Capability{
			capabilities.CapabilityRead,
			capabilities.CapabilityTimeTravel,
		},
		Available: false, // Not implemented yet
		Priority:  3,
	})

	return router
}
