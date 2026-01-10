// Package redflag contains Red-Flag tests that prove the system correctly refuses
// unsafe or invalid operations. These tests must be written BEFORE implementation.
//
// Per docs/test.md: "Red-Flag tests validate invariant enforcement,
// capability boundaries, and trust guarantees."
package redflag

// This package contains Red-Flag tests organized by component:
// - capability_test.go: Tests for capability enforcement
// - table_test.go: Tests for virtual table validation
// - auth_test.go: Tests for authentication failures
// - planner_test.go: Tests for planner rejections
// - query_test.go: Tests for query rejection
