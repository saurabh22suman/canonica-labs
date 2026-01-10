// Package cli provides the command-line interface for canonica.
// The CLI is a control interface for configuring, validating, and querying.
package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/canonica-labs/canonica/internal/config"
)

// Exit codes as defined in docs/canonic-cli-spec.md
const (
	ExitSuccess    = 0
	ExitValidation = 1
	ExitAuth       = 2
	ExitEngine     = 3
	ExitInternal   = 4
)

// Version information (set at build time)
var (
	Version   = "0.1.0"
	GitCommit = "unknown"
	BuildDate = "unknown"
)

// CLI holds the command-line interface state.
type CLI struct {
	rootCmd *cobra.Command
	cfg     *config.Config

	// Global flags
	configPath string
	endpoint   string
	token      string
	jsonOutput bool
	quiet      bool
	debug      bool
}

// New creates a new CLI instance.
func New() *CLI {
	cli := &CLI{}
	cli.rootCmd = cli.newRootCmd()
	return cli
}

// Execute runs the CLI.
func (c *CLI) Execute() int {
	if err := c.rootCmd.Execute(); err != nil {
		return ExitInternal
	}
	return ExitSuccess
}

func (c *CLI) newRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "canonic",
		Short: "Canonica - Unified Lakehouse Access Layer",
		Long: `Canonica is a control plane for unified query access across lakehouse formats.

It provides:
  • Virtual table abstraction over Delta, Iceberg, and Parquet
  • Capability-based access control
  • Deterministic engine routing
  • Query validation and explanation

This CLI is a control interface for configuration, validation, and diagnostics.`,
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return c.initConfig()
		},
	}

	// Global flags
	cmd.PersistentFlags().StringVar(&c.configPath, "config", "", "config file (default: ~/.canonic/config.yaml)")
	cmd.PersistentFlags().StringVar(&c.endpoint, "endpoint", "", "control plane endpoint")
	cmd.PersistentFlags().StringVar(&c.token, "token", "", "auth token (overrides config)")
	cmd.PersistentFlags().BoolVar(&c.jsonOutput, "json", false, "machine-readable JSON output")
	cmd.PersistentFlags().BoolVar(&c.quiet, "quiet", false, "suppress non-essential output")
	cmd.PersistentFlags().BoolVar(&c.debug, "debug", false, "verbose debug logs")

	// Add command groups
	cmd.AddCommand(c.newAuthCmd())
	cmd.AddCommand(c.newTableCmd())
	cmd.AddCommand(c.newQueryCmd())
	cmd.AddCommand(c.newEngineCmd())
	cmd.AddCommand(c.newDoctorCmd())
	cmd.AddCommand(c.newVersionCmd())
	// Phase 5 commands
	cmd.AddCommand(c.newBootstrapCmd())
	cmd.AddCommand(c.newStatusCmd())
	cmd.AddCommand(c.newAuditCmd())

	return cmd
}

func (c *CLI) initConfig() error {
	cfg, err := config.Load(c.configPath)
	if err != nil {
		return err
	}
	c.cfg = cfg

	// Override with flags
	if c.endpoint != "" {
		c.cfg.Endpoint = c.endpoint
	}
	if c.token != "" {
		c.cfg.Auth.Token = c.token
	}

	return nil
}

// Helper functions for output

func (c *CLI) printf(format string, args ...interface{}) {
	if !c.quiet {
		fmt.Printf(format, args...)
	}
}

func (c *CLI) println(args ...interface{}) {
	if !c.quiet {
		fmt.Println(args...)
	}
}

func (c *CLI) errorf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format, args...)
}

func (c *CLI) debugf(format string, args ...interface{}) {
	if c.debug {
		fmt.Printf("[DEBUG] "+format, args...)
	}
}

// newGatewayClient creates a new gateway client with current config.
func (c *CLI) newGatewayClient() *GatewayClient {
	return NewGatewayClient(c.cfg.Endpoint, c.cfg.Auth.Token)
}

