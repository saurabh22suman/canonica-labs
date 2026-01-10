// Package config provides configuration loading for the canonic CLI and gateway.
package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/viper"
)

// Config holds the application configuration.
type Config struct {
	// Endpoint is the control plane URL
	Endpoint string `mapstructure:"endpoint"`

	// Auth configuration
	Auth AuthConfig `mapstructure:"auth"`

	// Database configuration (for gateway)
	Database DatabaseConfig `mapstructure:"database"`

	// Engines configuration
	Engines EnginesConfig `mapstructure:"engines"`

	// Logging configuration
	Logging LoggingConfig `mapstructure:"logging"`

	// Server configuration (for gateway)
	Server ServerConfig `mapstructure:"server"`
}

// AuthConfig holds authentication configuration.
type AuthConfig struct {
	Token string `mapstructure:"token"`
}

// DatabaseConfig holds PostgreSQL configuration.
type DatabaseConfig struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	Name     string `mapstructure:"name"`
	SSLMode  string `mapstructure:"sslmode"`
}

// EnginesConfig holds engine configurations.
type EnginesConfig struct {
	DuckDB DuckDBConfig `mapstructure:"duckdb"`
	Trino  TrinoConfig  `mapstructure:"trino"`
	Spark  SparkConfig  `mapstructure:"spark"`
}

// DuckDBConfig holds DuckDB configuration.
type DuckDBConfig struct {
	Enabled  bool   `mapstructure:"enabled"`
	Database string `mapstructure:"database"`
}

// TrinoConfig holds Trino configuration.
type TrinoConfig struct {
	Enabled bool   `mapstructure:"enabled"`
	Host    string `mapstructure:"host"`
	Port    int    `mapstructure:"port"`
	Catalog string `mapstructure:"catalog"`
}

// SparkConfig holds Spark configuration.
type SparkConfig struct {
	Enabled bool `mapstructure:"enabled"`
}

// LoggingConfig holds logging configuration.
type LoggingConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

// ServerConfig holds HTTP server configuration.
type ServerConfig struct {
	Port         int    `mapstructure:"port"`
	ReadTimeout  string `mapstructure:"readTimeout"`
	WriteTimeout string `mapstructure:"writeTimeout"`
}

// DefaultConfig returns a configuration with default values.
func DefaultConfig() *Config {
	return &Config{
		Endpoint: "http://localhost:8080",
		Auth: AuthConfig{
			Token: "",
		},
		Database: DatabaseConfig{
			Host:     "localhost",
			Port:     5432,
			User:     "canonica",
			Password: "canonica_dev",
			Name:     "canonica",
			SSLMode:  "disable",
		},
		Engines: EnginesConfig{
			DuckDB: DuckDBConfig{
				Enabled:  true,
				Database: ":memory:",
			},
			Trino: TrinoConfig{
				Enabled: false,
				Host:    "localhost",
				Port:    8080,
				Catalog: "hive",
			},
			Spark: SparkConfig{
				Enabled: false,
			},
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "json",
		},
		Server: ServerConfig{
			Port:         8080,
			ReadTimeout:  "30s",
			WriteTimeout: "30s",
		},
	}
}

// Load loads configuration from file and environment.
func Load(configPath string) (*Config, error) {
	v := viper.New()

	// Set defaults
	setDefaults(v)

	// Config file
	if configPath != "" {
		v.SetConfigFile(configPath)
	} else {
		// Default config locations
		home, err := os.UserHomeDir()
		if err == nil {
			v.AddConfigPath(filepath.Join(home, ".canonic"))
		}
		v.AddConfigPath(".")
		v.SetConfigName("config")
		v.SetConfigType("yaml")
	}

	// Environment variables
	v.SetEnvPrefix("CANONICA")
	v.AutomaticEnv()

	// Read config file
	if err := v.ReadInConfig(); err != nil {
		// Config file is optional
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("error reading config: %w", err)
		}
	}

	// Unmarshal
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("error parsing config: %w", err)
	}

	return &cfg, nil
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("endpoint", "http://localhost:8080")
	v.SetDefault("auth.token", "")
	v.SetDefault("database.host", "localhost")
	v.SetDefault("database.port", 5432)
	v.SetDefault("database.user", "canonica")
	v.SetDefault("database.password", "canonica_dev")
	v.SetDefault("database.name", "canonica")
	v.SetDefault("database.sslmode", "disable")
	v.SetDefault("engines.duckdb.enabled", true)
	v.SetDefault("engines.duckdb.database", ":memory:")
	v.SetDefault("engines.trino.enabled", false)
	v.SetDefault("engines.spark.enabled", false)
	v.SetDefault("logging.level", "info")
	v.SetDefault("logging.format", "json")
	v.SetDefault("server.port", 8080)
	v.SetDefault("server.readTimeout", "30s")
	v.SetDefault("server.writeTimeout", "30s")
}
