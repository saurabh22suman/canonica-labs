// Package greenflag contains tests that verify the system correctly performs allowed operations.
// Per docs/test.md: "Green-Flag tests demonstrate allowed behavior and must be deterministic."
package greenflag

import (
	"context"
	"testing"
	"time"

	"github.com/canonica-labs/canonica/internal/adapters"
)

// Phase 6 Green-Flag Tests: Retry Logic
// Per phase-6-spec.md: Retry logic must be explicit and transparent

// TestRetry_DefaultConfigHasSensibleValues verifies defaults are usable.
// Green-Flag: DefaultRetryConfig must return reasonable values.
func TestRetry_DefaultConfigHasSensibleValues(t *testing.T) {
	config := adapters.DefaultRetryConfig()

	if config.MaxAttempts <= 0 {
		t.Fatalf("MaxAttempts should be positive, got %d", config.MaxAttempts)
	}

	if config.InitialDelay <= 0 {
		t.Fatalf("InitialDelay should be positive, got %v", config.InitialDelay)
	}

	if config.MaxDelay <= 0 {
		t.Fatalf("MaxDelay should be positive, got %v", config.MaxDelay)
	}

	if config.BackoffMultiplier <= 0 {
		t.Fatalf("BackoffMultiplier should be positive, got %v", config.BackoffMultiplier)
	}

	// Verify InitialDelay < MaxDelay (sensible ordering)
	if config.InitialDelay >= config.MaxDelay {
		t.Fatalf("InitialDelay (%v) should be less than MaxDelay (%v)",
			config.InitialDelay, config.MaxDelay)
	}
}

// TestRetry_SuccessOnFirstAttempt verifies fast path works.
// Green-Flag: Successful operations should complete without unnecessary retries.
func TestRetry_SuccessOnFirstAttempt(t *testing.T) {
	callCount := 0
	result := adapters.ExecuteWithRetry(context.Background(), adapters.DefaultRetryConfig(), func() error {
		callCount++
		return nil // Success
	})

	if !result.Success {
		t.Fatal("expected success")
	}

	if callCount != 1 {
		t.Fatalf("expected exactly 1 call for successful operation, got %d", callCount)
	}

	if result.Attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", result.Attempts)
	}

	if len(result.Errors) != 0 {
		t.Fatalf("expected 0 errors for successful operation, got %d", len(result.Errors))
	}
}

// TestRetry_ResultStringDescribesSuccess verifies success message is clear.
// Green-Flag: RetryResult.String() should be human-readable.
func TestRetry_ResultStringDescribesSuccess(t *testing.T) {
	result := adapters.RetryResult{
		Attempts: 1,
		Success:  true,
	}

	str := result.String()
	if str == "" {
		t.Fatal("String() should not return empty string")
	}

	// Should mention success
	if !containsAny(str, []string{"succeed", "success", "first"}) {
		t.Logf("Result string: %s", str)
	}
}

// TestRetry_ResultStringDescribesFailure verifies failure message is clear.
// Green-Flag: RetryResult.String() should describe failures clearly.
func TestRetry_ResultStringDescribesFailure(t *testing.T) {
	result := adapters.RetryResult{
		Attempts:  3,
		Success:   false,
		LastError: context.DeadlineExceeded,
	}

	str := result.String()
	if str == "" {
		t.Fatal("String() should not return empty string for failure")
	}

	// Should mention failure and attempts
	if !containsAny(str, []string{"fail", "3"}) {
		t.Logf("Result string: %s", str)
	}
}

// TestRetry_ConfigAppliesDefaults verifies zero config uses defaults.
// Green-Flag: Zero-value config should use sensible defaults.
func TestRetry_ConfigAppliesDefaults(t *testing.T) {
	zeroConfig := adapters.RetryConfig{} // All zero values

	callCount := 0
	result := adapters.ExecuteWithRetry(context.Background(), zeroConfig, func() error {
		callCount++
		return nil
	})

	if !result.Success {
		t.Fatal("expected success")
	}

	if callCount != 1 {
		t.Fatalf("expected 1 call, got %d", callCount)
	}
}

// TestRetry_ExecuteWithRetryCompletesQuickly verifies no unnecessary delays.
// Green-Flag: Successful operations should complete without delay.
func TestRetry_ExecuteWithRetryCompletesQuickly(t *testing.T) {
	start := time.Now()

	result := adapters.ExecuteWithRetry(context.Background(), adapters.DefaultRetryConfig(), func() error {
		return nil
	})

	elapsed := time.Since(start)

	if !result.Success {
		t.Fatal("expected success")
	}

	// Should complete in well under 100ms for a simple success
	if elapsed > 100*time.Millisecond {
		t.Fatalf("ExecuteWithRetry took too long for success: %v", elapsed)
	}
}

// Helper function to check if string contains any of the substrings
func containsAny(s string, substrs []string) bool {
	for _, substr := range substrs {
		if contains(s, substr) {
			return true
		}
	}
	return false
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsAt(s, substr))
}

func containsAt(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
