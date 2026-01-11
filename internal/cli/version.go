package cli

import (
	"context"
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
)

func (c *CLI) newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Display version information",
		Long:  `Display CLI and server version information.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.runVersion()
		},
	}
}

func (c *CLI) runVersion() error {
	info := VersionInfo{
		Version:   Version,
		GitCommit: GitCommit,
		BuildDate: BuildDate,
		GoVersion: runtime.Version(),
		OS:        runtime.GOOS,
		Arch:      runtime.GOARCH,
	}

	// Query server version if gateway endpoint is configured
	var serverVersion string
	var serverStatus string
	if c.cfg != nil && c.cfg.Endpoint != "" {
		client := c.newGatewayClient()
		ctx := context.Background()
		if health, err := client.GetHealthInfo(ctx); err == nil {
			serverVersion = health.Version
			serverStatus = health.Status
		} else {
			serverStatus = "unavailable"
		}
	} else {
		serverStatus = "not configured"
	}

	if c.jsonOutput {
		// Include server info in JSON output
		output := struct {
			VersionInfo
			Server struct {
				Version string `json:"version,omitempty"`
				Status  string `json:"status"`
			} `json:"server"`
		}{
			VersionInfo: info,
		}
		output.Server.Version = serverVersion
		output.Server.Status = serverStatus
		return c.outputJSON(output)
	}

	c.println("Canonica CLI")
	c.printf("  Version:    %s\n", info.Version)
	c.printf("  Git Commit: %s\n", info.GitCommit)
	c.printf("  Build Date: %s\n", info.BuildDate)
	c.printf("  Go Version: %s\n", info.GoVersion)
	c.printf("  OS/Arch:    %s/%s\n", info.OS, info.Arch)

	c.println("")
	c.println("Server:")
	if serverVersion != "" {
		c.printf("  Version: %s\n", serverVersion)
		c.printf("  Status:  %s\n", serverStatus)
	} else {
		c.printf("  Status: %s\n", serverStatus)
	}

	return nil
}

// VersionInfo represents version information for JSON output.
type VersionInfo struct {
	Version   string `json:"version"`
	GitCommit string `json:"git_commit"`
	BuildDate string `json:"build_date"`
	GoVersion string `json:"go_version"`
	OS        string `json:"os"`
	Arch      string `json:"arch"`
}

// SetVersionInfo sets the version information (called from main).
func SetVersionInfo(version, commit, date string) {
	if version != "" {
		Version = version
	}
	if commit != "" {
		GitCommit = commit
	}
	if date != "" {
		BuildDate = date
	}
}

func init() {
	// Set default build info if not set by ldflags
	if GitCommit == "" || GitCommit == "unknown" {
		GitCommit = "dev"
	}
	if BuildDate == "" || BuildDate == "unknown" {
		BuildDate = "unknown"
	}
}

// GetVersionString returns a formatted version string.
func GetVersionString() string {
	return fmt.Sprintf("canonic version %s (commit: %s, built: %s)",
		Version, GitCommit, BuildDate)
}
