// Package auth provides authentication for the canonica control plane.
// MVP uses static token authentication. See tracker.md T001 for JWT implementation.
//
// Per docs/plan.md: "Token-based auth, role → table mapping."
package auth

import (
	"context"
	"sync"
	"time"

	"github.com/canonica-labs/canonica/internal/errors"
)

// User represents an authenticated user.
type User struct {
	// ID is the unique identifier for this user.
	ID string `json:"id"`

	// Name is the display name of the user.
	Name string `json:"name"`

	// Roles are the roles assigned to this user.
	Roles []string `json:"roles"`

	// ExpiresAt is when the authentication expires (for token-based auth).
	ExpiresAt time.Time `json:"expires_at,omitempty"`
}

// IsExpired checks if the user's authentication has expired.
func (u *User) IsExpired() bool {
	if u.ExpiresAt.IsZero() {
		return false
	}
	return time.Now().After(u.ExpiresAt)
}

// HasRole checks if the user has the given role.
func (u *User) HasRole(role string) bool {
	for _, r := range u.Roles {
		if r == role {
			return true
		}
	}
	return false
}

// Authenticator validates authentication tokens and returns user information.
type Authenticator interface {
	// ValidateToken validates a token and returns the authenticated user.
	// Returns an error if the token is invalid or expired.
	ValidateToken(ctx context.Context, token string) (*User, error)
}

// StaticTokenAuthenticator implements Authenticator using static tokens from configuration.
// This is the MVP implementation. See tracker.md T001 for JWT.
type StaticTokenAuthenticator struct {
	mu     sync.RWMutex
	tokens map[string]*User
}

// NewStaticTokenAuthenticator creates a new static token authenticator.
func NewStaticTokenAuthenticator() *StaticTokenAuthenticator {
	return &StaticTokenAuthenticator{
		tokens: make(map[string]*User),
	}
}

// RegisterToken adds a token-to-user mapping.
func (a *StaticTokenAuthenticator) RegisterToken(token string, user *User) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.tokens[token] = user
}

// ValidateToken validates a static token.
func (a *StaticTokenAuthenticator) ValidateToken(ctx context.Context, token string) (*User, error) {
	if token == "" {
		return nil, errors.NewAuthFailed("token required")
	}

	a.mu.RLock()
	user, ok := a.tokens[token]
	a.mu.RUnlock()

	if !ok {
		return nil, errors.NewAuthFailed("invalid token")
	}

	if user.IsExpired() {
		return nil, errors.NewAuthExpired()
	}

	return user, nil
}

// contextKey is a custom type for context keys to avoid collisions.
type contextKey string

const userContextKey contextKey = "canonica_user"

// ContextWithUser returns a new context with the user attached.
func ContextWithUser(ctx context.Context, user *User) context.Context {
	return context.WithValue(ctx, userContextKey, user)
}

// UserFromContext extracts the user from the context.
// Returns nil if no user is attached.
func UserFromContext(ctx context.Context) *User {
	user, _ := ctx.Value(userContextKey).(*User)
	return user
}
