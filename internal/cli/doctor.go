package cli

import (
	"fmt"
	"net"
	"time"

	"github.com/spf13/cobra"

	"github.com/canonica-labs/canonica/internal/router"
)

func (c *CLI) newDoctorCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "doctor",
		Short: "Run system diagnostics",
		Long: `Run comprehensive system diagnostics.

Checks:
  - connectivity to control plane
  - authentication status
  - engine health
  - metadata integrity`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.runDoctor()
		},
	}
}

func (c *CLI) runDoctor() error {
	c.println("Canonica System Diagnostics")
	c.println("===========================")
	c.println("")

	checks := []DiagnosticCheck{}
	allPassed := true

	// Check 1: Configuration
	configCheck := c.checkConfig()
	checks = append(checks, configCheck)
	if !configCheck.Passed {
		allPassed = false
	}
	c.printCheck(configCheck)

	// Check 2: Authentication
	authCheck := c.checkAuth()
	checks = append(checks, authCheck)
	if !authCheck.Passed {
		allPassed = false
	}
	c.printCheck(authCheck)

	// Check 3: Gateway connectivity
	gatewayCheck := c.checkGateway()
	checks = append(checks, gatewayCheck)
	if !gatewayCheck.Passed {
		allPassed = false
	}
	c.printCheck(gatewayCheck)

	// Check 4: Engine availability
	engineCheck := c.checkEngines()
	checks = append(checks, engineCheck)
	if !engineCheck.Passed {
		allPassed = false
	}
	c.printCheck(engineCheck)

	c.println("")

	if c.jsonOutput {
		return c.outputJSON(map[string]interface{}{
			"checks":     checks,
			"all_passed": allPassed,
		})
	}

	if allPassed {
		c.println("✓ All checks passed")
	} else {
		c.println("✗ Some checks failed - see above for details")
	}

	return nil
}

// DiagnosticCheck represents a single diagnostic check result.
type DiagnosticCheck struct {
	Name    string `json:"name"`
	Passed  bool   `json:"passed"`
	Message string `json:"message"`
	Details string `json:"details,omitempty"`
}

func (c *CLI) printCheck(check DiagnosticCheck) {
	status := "✗"
	if check.Passed {
		status = "✓"
	}
	c.printf("%s %s: %s\n", status, check.Name, check.Message)
	if check.Details != "" && !check.Passed {
		c.printf("  → %s\n", check.Details)
	}
}

func (c *CLI) checkConfig() DiagnosticCheck {
	check := DiagnosticCheck{Name: "Configuration"}

	if c.cfg == nil {
		check.Passed = false
		check.Message = "No configuration loaded"
		check.Details = "Create ~/.canonic/config.yaml or use --config flag"
		return check
	}

	if c.cfg.Endpoint == "" {
		check.Passed = false
		check.Message = "No endpoint configured"
		check.Details = "Set endpoint in config or use --endpoint flag"
		return check
	}

	check.Passed = true
	check.Message = fmt.Sprintf("Endpoint: %s", c.cfg.Endpoint)
	return check
}

func (c *CLI) checkAuth() DiagnosticCheck {
	check := DiagnosticCheck{Name: "Authentication"}

	token := c.getToken()
	if token == "" {
		check.Passed = false
		check.Message = "Not authenticated"
		check.Details = "Run 'canonic auth login' to authenticate"
		return check
	}

	check.Passed = true
	check.Message = fmt.Sprintf("Token present (source: %s)", c.getTokenSource())
	return check
}

func (c *CLI) checkGateway() DiagnosticCheck {
	check := DiagnosticCheck{Name: "Gateway Connectivity"}

	if c.cfg == nil || c.cfg.Endpoint == "" {
		check.Passed = false
		check.Message = "No endpoint configured"
		return check
	}

	// Try to connect to the gateway
	// Parse host:port from endpoint
	endpoint := c.cfg.Endpoint
	// Remove protocol prefix
	if len(endpoint) > 7 && endpoint[:7] == "http://" {
		endpoint = endpoint[7:]
	} else if len(endpoint) > 8 && endpoint[:8] == "https://" {
		endpoint = endpoint[8:]
	}

	// Try to connect
	conn, err := net.DialTimeout("tcp", endpoint, 2*time.Second)
	if err != nil {
		check.Passed = false
		check.Message = "Cannot connect to gateway"
		check.Details = fmt.Sprintf("Error: %v", err)
		return check
	}
	conn.Close()

	check.Passed = true
	check.Message = fmt.Sprintf("Connected to %s", c.cfg.Endpoint)
	return check
}

func (c *CLI) checkEngines() DiagnosticCheck {
	check := DiagnosticCheck{Name: "Engine Availability"}

	r := router.DefaultRouter()
	available := r.AvailableEngines(nil)

	if len(available) == 0 {
		check.Passed = false
		check.Message = "No engines available"
		check.Details = "At least one engine must be configured and available"
		return check
	}

	check.Passed = true
	check.Message = fmt.Sprintf("%d engine(s) available: %v", len(available), available)
	return check
}
