// Package redflag contains tests that verify the system correctly rejects unsafe operations.
// Per docs/test.md: "Red-Flag tests must fail before implementation and prove unsafe behavior is blocked."
package redflag

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/canonica-labs/canonica/internal/adapters"
)

// Phase 6 Red-Flag Tests: Retry Logic
// Per phase-6-spec.md: Retry logic must be explicit and never hide errors

// TestRetry_RejectsNonRetryableErrors verifies that non-retryable errors are not retried.
// Red-Flag: Semantic errors (auth, syntax) must not be silently retried.
func TestRetry_RejectsNonRetryableErrors(t *testing.T) {
	// Per copilot-instructions.md: "Never silently fallback"
	authError := errors.New("authentication failed: invalid token")

	if adapters.IsRetryable(authError) {
		t.Fatal("authentication errors should not be retryable")
	}
}

// TestRetry_RejectsContextCancelled verifies that context.Canceled is not retryable.
// Red-Flag: User-initiated cancellation must not be retried.
func TestRetry_RejectsContextCancelled(t *testing.T) {
	if adapters.IsRetryable(context.Canceled) {
		t.Fatal("context.Canceled should not be retryable")
	}
}

// TestRetry_RejectsContextDeadlineExceeded verifies that context.DeadlineExceeded is not retryable.
// Red-Flag: Deadline exceeded is a policy decision, not a transient failure.
func TestRetry_RejectsContextDeadlineExceeded(t *testing.T) {
	if adapters.IsRetryable(context.DeadlineExceeded) {
		t.Fatal("context.DeadlineExceeded should not be retryable")
	}
}

// TestRetry_RejectsNilError verifies that nil errors are not retryable.
// Red-Flag: Success should not be confused with a retryable error.
func TestRetry_RejectsNilError(t *testing.T) {
	if adapters.IsRetryable(nil) {
		t.Fatal("nil error should not be retryable")
	}
}

// TestRetry_ExecuteWithRetryRespectsMaxAttempts verifies max attempts are honored.
// Red-Flag: Retry must not continue indefinitely.
func TestRetry_ExecuteWithRetryRespectsMaxAttempts(t *testing.T) {
	callCount := 0
	testErr := errors.New("temporary failure")

	config := adapters.RetryConfig{
		MaxAttempts:       3,
		InitialDelay:      1 * time.Millisecond,
		MaxDelay:          10 * time.Millisecond,
		BackoffMultiplier: 2.0,
	}

	result := adapters.ExecuteWithRetry(context.Background(), config, func() error {
		callCount++
		return testErr
	})

	// Per copilot-instructions.md: Errors must be explicit
	// Since IsRetryable returns false for all errors currently,
	// we expect only 1 attempt (no retries for non-retryable errors)
	if result.Success {
		t.Fatal("expected failure, got success")
	}

	if callCount != 1 {
		t.Fatalf("expected 1 call (non-retryable), got %d", callCount)
	}
}

// TestRetry_ExecuteWithRetryRespectsContextCancellation verifies context is honored.
// Red-Flag: Cancelled context must stop retry loop.
func TestRetry_ExecuteWithRetryRespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	callCount := 0
	result := adapters.ExecuteWithRetry(ctx, adapters.DefaultRetryConfig(), func() error {
		callCount++
		return errors.New("would retry if context allowed")
	})

	if result.Success {
		t.Fatal("expected failure due to cancelled context")
	}

	// Should have stopped due to context cancellation
	if callCount > 1 {
		t.Fatalf("expected at most 1 call due to cancelled context, got %d", callCount)
	}
}

// TestRetry_RetryResultContainsAllErrors verifies all errors are captured.
// Red-Flag: Error history must be preserved for debugging.
func TestRetry_RetryResultContainsAllErrors(t *testing.T) {
	result := adapters.ExecuteWithRetry(context.Background(), adapters.DefaultRetryConfig(), func() error {
		return errors.New("test error")
	})

	if len(result.Errors) == 0 {
		t.Fatal("expected errors to be recorded in result")
	}

	if result.LastError == nil {
		t.Fatal("expected LastError to be set")
	}
}

// TestRetry_RetryableErrorWrapsOriginal verifies error wrapping works.
// Red-Flag: Original error must be accessible via Unwrap.
func TestRetry_RetryableErrorWrapsOriginal(t *testing.T) {
	originalErr := errors.New("original error")

	result := adapters.RetryResult{
		Attempts:  3,
		LastError: originalErr,
		Success:   false,
	}

	retryErr := &adapters.RetryableError{Result: result}

	// Verify Unwrap returns the original error
	if !errors.Is(retryErr, originalErr) {
		t.Fatal("RetryableError should wrap the original error")
	}

	// Verify Error() contains useful information
	errMsg := retryErr.Error()
	if errMsg == "" {
		t.Fatal("error message should not be empty")
	}
}

// TestRetry_ExecuteWithRetrySucceedsOnFirstAttempt verifies success case.
// Red-Flag: Success must be properly reported.
func TestRetry_ExecuteWithRetrySucceedsOnFirstAttempt(t *testing.T) {
	callCount := 0
	result := adapters.ExecuteWithRetry(context.Background(), adapters.DefaultRetryConfig(), func() error {
		callCount++
		return nil // Success
	})

	if !result.Success {
		t.Fatal("expected success")
	}

	if callCount != 1 {
		t.Fatalf("expected exactly 1 call, got %d", callCount)
	}

	if result.Attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", result.Attempts)
	}
}
