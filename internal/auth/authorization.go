// Package auth provides authorization for role → table → capability mapping.
// This implements the deny-by-default authorization model per phase-2-spec.md §4.
//
// Core Principle: Absence of permission is denial.
package auth

import (
	"context"
	"fmt"
	"sync"

	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/errors"
)

// Permission represents a single permission grant.
// Format: Role → Table → Capability
type Permission struct {
	Role       string
	Table      string
	Capability capabilities.Capability
}

// AuthorizationService manages role → table → capability mappings.
// Per phase-2-spec.md: "Absence of permission is denial."
type AuthorizationService struct {
	mu          sync.RWMutex
	permissions map[string]map[string][]capabilities.Capability // role → table → capabilities
}

// NewAuthorizationService creates a new authorization service with deny-by-default.
func NewAuthorizationService() *AuthorizationService {
	return &AuthorizationService{
		permissions: make(map[string]map[string][]capabilities.Capability),
	}
}

// GrantAccess grants a capability on a table to a role.
// Per phase-2-spec.md: explicit grants only.
func (s *AuthorizationService) GrantAccess(role, table string, cap capabilities.Capability) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.permissions[role] == nil {
		s.permissions[role] = make(map[string][]capabilities.Capability)
	}

	// Check if capability already exists
	for _, existingCap := range s.permissions[role][table] {
		if existingCap == cap {
			return // Already granted
		}
	}

	s.permissions[role][table] = append(s.permissions[role][table], cap)
}

// RevokeAccess removes a capability from a role on a table.
func (s *AuthorizationService) RevokeAccess(role, table string, cap capabilities.Capability) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.permissions[role] == nil {
		return
	}

	caps := s.permissions[role][table]
	for i, existingCap := range caps {
		if existingCap == cap {
			s.permissions[role][table] = append(caps[:i], caps[i+1:]...)
			return
		}
	}
}

// Authorize checks if the user has the required capability on ALL specified tables.
// Per phase-2-spec.md §4:
//   - Authorization is evaluated per table referenced in a query
//   - A query referencing multiple tables requires authorization on ALL tables
//   - Partial authorization is not allowed
//
// Returns nil if authorized, error if denied.
func (s *AuthorizationService) Authorize(ctx context.Context, user *User, tables []string, requiredCap capabilities.Capability) error {
	if user == nil {
		return errors.NewAccessDenied("", string(requiredCap), "no user context")
	}

	// Check each table
	for _, table := range tables {
		if !s.hasPermission(user.Roles, table, requiredCap) {
			// Per phase-2-spec.md: "clearly state the unauthorized table"
			// "identify the missing capability"
			return errors.NewAccessDenied(table, string(requiredCap),
				fmt.Sprintf("role(s) %v lack %s permission on %s", user.Roles, requiredCap, table))
		}
	}

	return nil
}

// hasPermission checks if any of the given roles has the required capability on the table.
// Per phase-2-spec.md: "Absence of permission is denial."
func (s *AuthorizationService) hasPermission(roles []string, table string, requiredCap capabilities.Capability) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check each role the user has
	for _, role := range roles {
		rolePerms, ok := s.permissions[role]
		if !ok {
			continue // Role has no permissions
		}

		tableCaps, ok := rolePerms[table]
		if !ok {
			continue // Role has no permissions on this table
		}

		for _, cap := range tableCaps {
			if cap == requiredCap {
				return true
			}
		}
	}

	return false // Deny by default
}

// HasAccess is a convenience method to check a single table.
func (s *AuthorizationService) HasAccess(user *User, table string, requiredCap capabilities.Capability) bool {
	return s.hasPermission(user.Roles, table, requiredCap)
}

// GetPermissions returns all permissions for a role (for debugging/admin).
func (s *AuthorizationService) GetPermissions(role string) map[string][]capabilities.Capability {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string][]capabilities.Capability)
	if rolePerms, ok := s.permissions[role]; ok {
		for table, caps := range rolePerms {
			result[table] = append([]capabilities.Capability{}, caps...)
		}
	}
	return result
}
