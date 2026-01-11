// Package adapters provides the engine adapter interface and utilities.
//
// Per phase-6-spec.md: Retry logic for transient failures.
// Per docs/plan.md: "Adapters are stateless, replaceable, thin."
// Per copilot-instructions.md: "No silent retries. No hidden fallbacks."
//
// IMPORTANT: This retry utility is EXPLICIT - it returns a RetryResult
// that clearly indicates whether retries occurred and what happened.
package adapters

import (
	"context"
	"fmt"
	"time"
)

// RetryConfig configures retry behavior.
type RetryConfig struct {
	// MaxAttempts is the maximum number of attempts (including first try).
	// Default: 3
	MaxAttempts int

	// InitialDelay is the initial delay between retries.
	// Default: 100ms
	InitialDelay time.Duration

	// MaxDelay is the maximum delay between retries.
	// Default: 5s
	MaxDelay time.Duration

	// BackoffMultiplier is the multiplier for exponential backoff.
	// Default: 2.0
	BackoffMultiplier float64
}

// DefaultRetryConfig returns the default retry configuration.
// Per phase-6-spec.md: Sensible defaults for transient failures.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts:       3,
		InitialDelay:      100 * time.Millisecond,
		MaxDelay:          5 * time.Second,
		BackoffMultiplier: 2.0,
	}
}

// RetryResult contains the result of a retry operation.
// Per copilot-instructions.md: "Errors are explicit, never hidden."
type RetryResult struct {
	// Attempts is the number of attempts made.
	Attempts int

	// LastError is the last error encountered (nil if successful).
	LastError error

	// Errors contains all errors from each attempt.
	Errors []error

	// Success indicates whether the operation ultimately succeeded.
	Success bool
}

// String provides a human-readable summary of the retry result.
func (r RetryResult) String() string {
	if r.Success {
		if r.Attempts == 1 {
			return "succeeded on first attempt"
		}
		return fmt.Sprintf("succeeded after %d attempts", r.Attempts)
	}
	return fmt.Sprintf("failed after %d attempts: %v", r.Attempts, r.LastError)
}

// RetryableError wraps an error with retry information.
// This allows callers to see both the original error and retry context.
type RetryableError struct {
	Result RetryResult
}

func (e *RetryableError) Error() string {
	return fmt.Sprintf("operation failed after %d attempts: %v", e.Result.Attempts, e.Result.LastError)
}

func (e *RetryableError) Unwrap() error {
	return e.Result.LastError
}

// IsRetryable determines if an error is likely transient and worth retrying.
// Per phase-6-spec.md: Only retry transient failures like network timeouts.
// Per docs/plan.md: "Never retry semantic errors."
//
// Returns true for:
//   - Connection timeouts
//   - Network errors
//   - Temporary unavailability
//
// Returns false for:
//   - Authentication errors
//   - Authorization errors
//   - Syntax errors
//   - Semantic validation errors
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}

	// Check for context cancellation/deadline - NOT retryable
	if err == context.Canceled || err == context.DeadlineExceeded {
		return false
	}

	// TODO: Add specific error type checks as we learn more about
	// actual failure modes from Trino and Spark drivers.
	// For MVP, we conservatively return false to avoid masking errors.
	//
	// Per copilot-instructions.md: "If unsure, code must fail."
	return false
}

// ExecuteWithRetry executes a function with retry logic.
// Per phase-6-spec.md: Explicit retry with full visibility into attempts.
//
// The function is NOT hidden or automatic - callers explicitly choose
// to use retry logic and receive full information about what happened.
//
// Usage:
//
//	result := adapters.ExecuteWithRetry(ctx, adapters.DefaultRetryConfig(), func() error {
//	    return adapter.CheckHealth(ctx)
//	})
//	if !result.Success {
//	    return fmt.Errorf("health check failed: %w", &adapters.RetryableError{Result: result})
//	}
func ExecuteWithRetry(ctx context.Context, config RetryConfig, fn func() error) RetryResult {
	// Apply defaults
	if config.MaxAttempts <= 0 {
		config.MaxAttempts = 3
	}
	if config.InitialDelay <= 0 {
		config.InitialDelay = 100 * time.Millisecond
	}
	if config.MaxDelay <= 0 {
		config.MaxDelay = 5 * time.Second
	}
	if config.BackoffMultiplier <= 0 {
		config.BackoffMultiplier = 2.0
	}

	result := RetryResult{
		Errors: make([]error, 0, config.MaxAttempts),
	}

	delay := config.InitialDelay

	for attempt := 1; attempt <= config.MaxAttempts; attempt++ {
		result.Attempts = attempt

		// Check context before each attempt
		if ctx.Err() != nil {
			result.LastError = ctx.Err()
			result.Errors = append(result.Errors, ctx.Err())
			return result
		}

		// Execute the function
		err := fn()
		if err == nil {
			result.Success = true
			return result
		}

		result.LastError = err
		result.Errors = append(result.Errors, err)

		// Check if error is retryable
		if !IsRetryable(err) {
			// Per copilot-instructions.md: "Never silently fallback"
			return result
		}

		// Don't sleep after last attempt
		if attempt < config.MaxAttempts {
			select {
			case <-ctx.Done():
				result.LastError = ctx.Err()
				result.Errors = append(result.Errors, ctx.Err())
				return result
			case <-time.After(delay):
				// Apply exponential backoff
				delay = time.Duration(float64(delay) * config.BackoffMultiplier)
				if delay > config.MaxDelay {
					delay = config.MaxDelay
				}
			}
		}
	}

	return result
}
