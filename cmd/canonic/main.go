// Package main is the entrypoint for the Canonic CLI.
// The CLI provides commands for table management, query execution,
// and system diagnostics.
package main

import (
	"os"

	"github.com/canonica-labs/canonica/internal/cli"
)

// Build-time variables (set via ldflags)
var (
	version   = "0.1.0"
	gitCommit = "unknown"
	buildDate = "unknown"
)

func main() {
	// Set version info
	cli.SetVersionInfo(version, gitCommit, buildDate)

	// Create and execute CLI
	c := cli.New()
	os.Exit(c.Execute())
}
