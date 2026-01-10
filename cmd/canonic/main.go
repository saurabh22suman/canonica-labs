// Package main is the entrypoint for the Canonic CLI.
// The CLI provides commands for table management, query execution,
// and system diagnostics.
package main

import (
	"fmt"
	"os"
)

// Exit codes as defined in docs/canonic-cli-spec.md
const (
	ExitSuccess    = 0
	ExitValidation = 1
	ExitAuth       = 2
	ExitEngine     = 3
	ExitInternal   = 4
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "canonic: %v\n", err)
		os.Exit(ExitInternal)
	}
}

func run() error {
	// TODO: Implement CLI commands
	// See tracker.md for implementation details
	fmt.Println("Canonic CLI - Not yet implemented")
	fmt.Println("Usage: canonic <command> [options]")
	fmt.Println("")
	fmt.Println("Commands:")
	fmt.Println("  auth      Authentication commands")
	fmt.Println("  table     Virtual table management")
	fmt.Println("  query     SQL query execution")
	fmt.Println("  engine    Engine inspection")
	fmt.Println("  doctor    System diagnostics")
	fmt.Println("  version   Display version info")
	return nil
}
