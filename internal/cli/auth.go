package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
)

func (c *CLI) newAuthCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "auth",
		Short: "Authentication commands",
		Long:  `Manage authentication with the canonica control plane.`,
	}

	cmd.AddCommand(c.newAuthLoginCmd())
	cmd.AddCommand(c.newAuthStatusCmd())
	cmd.AddCommand(c.newAuthLogoutCmd())

	return cmd
}

func (c *CLI) newAuthLoginCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "login",
		Short: "Authenticate with the control plane",
		Long: `Authenticate with the canonica control plane and store the token locally.

For MVP, this accepts a static token. See tracker.md T001 for JWT implementation.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.runAuthLogin()
		},
	}
}

func (c *CLI) runAuthLogin() error {
	// For MVP, we use static tokens
	// Prompt for token if not provided via flag
	var token string
	if c.token != "" {
		token = c.token
	} else {
		c.printf("Enter authentication token: ")
		_, err := fmt.Scanln(&token)
		if err != nil {
			return fmt.Errorf("failed to read token: %w", err)
		}
	}

	if token == "" {
		c.errorf("Error: token required\n")
		c.errorf("Suggestion: provide token via --token flag or enter when prompted\n")
		return fmt.Errorf("token required")
	}

	// Store token in config file
	configDir, err := c.getConfigDir()
	if err != nil {
		return err
	}

	if err := os.MkdirAll(configDir, 0700); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	tokenFile := filepath.Join(configDir, "token")
	if err := os.WriteFile(tokenFile, []byte(token), 0600); err != nil {
		return fmt.Errorf("failed to save token: %w", err)
	}

	c.println("✓ Authentication successful")
	c.printf("  Token saved to: %s\n", tokenFile)

	return nil
}

func (c *CLI) newAuthStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Display authentication status",
		Long:  `Display current authentication status including identity, roles, and token expiry.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.runAuthStatus()
		},
	}
}

func (c *CLI) runAuthStatus() error {
	token := c.getToken()

	if token == "" {
		if c.jsonOutput {
			output := map[string]interface{}{
				"authenticated": false,
				"error":         "no token found",
			}
			return c.outputJSON(output)
		}
		c.errorf("Not authenticated\n")
		c.errorf("Suggestion: run 'canonic auth login' to authenticate\n")
		return fmt.Errorf("not authenticated")
	}

	// For MVP with static tokens, we have limited info
	// In production with JWT, we would decode the token
	status := AuthStatus{
		Authenticated: true,
		TokenPresent:  true,
		TokenSource:   c.getTokenSource(),
	}

	if c.jsonOutput {
		return c.outputJSON(status)
	}

	c.println("Authentication Status:")
	c.println("  Authenticated: ✓")
	c.printf("  Token source: %s\n", status.TokenSource)
	c.println("  Note: Using static token (MVP). See tracker.md T001 for JWT.")

	return nil
}

func (c *CLI) newAuthLogoutCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "logout",
		Short: "Clear stored authentication",
		Long:  `Remove stored authentication token.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.runAuthLogout()
		},
	}
}

func (c *CLI) runAuthLogout() error {
	configDir, err := c.getConfigDir()
	if err != nil {
		return err
	}

	tokenFile := filepath.Join(configDir, "token")
	if err := os.Remove(tokenFile); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove token: %w", err)
	}

	c.println("✓ Logged out successfully")
	return nil
}

// AuthStatus represents authentication status for JSON output.
type AuthStatus struct {
	Authenticated bool      `json:"authenticated"`
	TokenPresent  bool      `json:"token_present"`
	TokenSource   string    `json:"token_source"`
	UserID        string    `json:"user_id,omitempty"`
	UserName      string    `json:"user_name,omitempty"`
	Roles         []string  `json:"roles,omitempty"`
	ExpiresAt     time.Time `json:"expires_at,omitempty"`
}

// Helper functions

func (c *CLI) getConfigDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("failed to get home directory: %w", err)
	}
	return filepath.Join(home, ".canonic"), nil
}

func (c *CLI) getToken() string {
	// Priority: flag > config > file
	if c.token != "" {
		return c.token
	}
	if c.cfg != nil && c.cfg.Auth.Token != "" {
		return c.cfg.Auth.Token
	}

	// Try to read from token file
	configDir, err := c.getConfigDir()
	if err != nil {
		return ""
	}
	tokenFile := filepath.Join(configDir, "token")
	data, err := os.ReadFile(tokenFile)
	if err != nil {
		return ""
	}
	return string(data)
}

func (c *CLI) getTokenSource() string {
	if c.token != "" {
		return "command-line flag"
	}
	if c.cfg != nil && c.cfg.Auth.Token != "" {
		return "config file"
	}
	return "token file (~/.canonic/token)"
}

func (c *CLI) outputJSON(v interface{}) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
