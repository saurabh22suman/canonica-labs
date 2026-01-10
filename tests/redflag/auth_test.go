package redflag

import (
	"context"
	"testing"
	"time"

	"github.com/canonica-labs/canonica/internal/auth"
	"github.com/canonica-labs/canonica/internal/errors"
)

// TestAuth_EmptyToken proves that empty tokens are rejected.
//
// Red-Flag: System MUST reject authentication with empty token.
func TestAuth_EmptyToken(t *testing.T) {
	// Arrange
	authenticator := auth.NewStaticTokenAuthenticator()
	ctx := context.Background()

	// Act
	_, err := authenticator.ValidateToken(ctx, "")

	// Assert: Authentication MUST fail
	if err == nil {
		t.Fatal("expected error for empty token, got nil")
	}

	// Assert: Error must be ErrAuthFailed
	if _, ok := err.(*errors.ErrAuthFailed); !ok {
		t.Fatalf("expected ErrAuthFailed, got %T: %v", err, err)
	}
}

// TestAuth_InvalidToken proves that invalid tokens are rejected.
//
// Red-Flag: System MUST reject authentication with unknown tokens.
func TestAuth_InvalidToken(t *testing.T) {
	// Arrange
	authenticator := auth.NewStaticTokenAuthenticator()
	// Register a valid token
	authenticator.RegisterToken("valid-token", &auth.User{
		ID:   "user-1",
		Name: "Test User",
	})
	ctx := context.Background()

	// Act: Try to authenticate with a different token
	_, err := authenticator.ValidateToken(ctx, "invalid-token")

	// Assert: Authentication MUST fail
	if err == nil {
		t.Fatal("expected error for invalid token, got nil")
	}
}

// TestAuth_ExpiredToken proves that expired tokens are rejected.
//
// Red-Flag: System MUST reject authentication with expired tokens.
func TestAuth_ExpiredToken(t *testing.T) {
	// Arrange
	authenticator := auth.NewStaticTokenAuthenticator()
	// Register an expired token
	authenticator.RegisterToken("expired-token", &auth.User{
		ID:        "user-1",
		Name:      "Test User",
		ExpiresAt: time.Now().Add(-1 * time.Hour), // Expired 1 hour ago
	})
	ctx := context.Background()

	// Act
	_, err := authenticator.ValidateToken(ctx, "expired-token")

	// Assert: Authentication MUST fail
	if err == nil {
		t.Fatal("expected error for expired token, got nil")
	}
}

// TestAuth_NoMetadataLeakage proves that authentication errors do not leak
// information about valid tokens or users.
//
// Red-Flag: Authentication failures MUST NOT reveal whether token exists.
func TestAuth_NoMetadataLeakage(t *testing.T) {
	// Arrange
	authenticator := auth.NewStaticTokenAuthenticator()
	authenticator.RegisterToken("valid-token", &auth.User{
		ID:   "user-1",
		Name: "Test User",
	})
	ctx := context.Background()

	// Act: Try two different invalid tokens
	_, err1 := authenticator.ValidateToken(ctx, "invalid-token-1")
	_, err2 := authenticator.ValidateToken(ctx, "invalid-token-2")

	// Assert: Both must fail
	if err1 == nil || err2 == nil {
		t.Fatal("expected both invalid tokens to fail")
	}

	// Assert: Error messages should be identical (no information leakage)
	// Both should say "invalid token" - not "token not found" vs "token expired"
	authErr1, ok1 := err1.(*errors.ErrAuthFailed)
	authErr2, ok2 := err2.(*errors.ErrAuthFailed)

	if !ok1 || !ok2 {
		t.Fatal("expected both errors to be ErrAuthFailed")
	}

	// The error messages for invalid tokens should be indistinguishable
	if authErr1.Reason != authErr2.Reason {
		t.Fatalf("error messages differ for invalid tokens (potential info leak): %q vs %q",
			authErr1.Reason, authErr2.Reason)
	}
}
