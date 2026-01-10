// Package greenflag contains Green-Flag tests that prove the system correctly
// succeeds on explicitly safe behavior. These tests validate happy paths.
//
// Per docs/test.md: "Green-Flag tests prove the system succeeds on
// explicitly safe behavior."
package greenflag

// This package contains Green-Flag tests organized by component:
// - capability_test.go: Tests for successful capability checks
// - table_test.go: Tests for valid table registration
// - auth_test.go: Tests for successful authentication
// - planner_test.go: Tests for successful query planning
// - query_test.go: Tests for successful query parsing
