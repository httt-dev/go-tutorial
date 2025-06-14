package config

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

// Config holds all configuration settings
type Config struct {
	OracleConnStr   string
	PostgresConnStr string
	TableOracle     string
	TablePostgres   string
	WhereClause     string
	KeyColumns      []string
	CompareColumns  []string
	IgnoreColumns   []string
	WorkerCount     int
	BatchSize       int
}

// LoadConfig loads configuration from .env file
func LoadConfig() (*Config, error) {
	// Load .env file
	if err := godotenv.Load(); err != nil {
		return nil, fmt.Errorf("error loading .env file: %v", err)
	}

	cfg := &Config{
		OracleConnStr:   os.Getenv("ORACLE_CONN_STR"),
		PostgresConnStr: os.Getenv("POSTGRES_CONN_STR"),
		TableOracle:     os.Getenv("TABLE_ORACLE"),
		TablePostgres:   os.Getenv("TABLE_POSTGRES"),
		WhereClause:     os.Getenv("WHERE_CLAUSE"),
		KeyColumns:      strings.Split(os.Getenv("KEY_COLUMNS"), ","),
		CompareColumns:  strings.Split(os.Getenv("COMPARE_COLUMNS"), ","),
		IgnoreColumns:   strings.Split(os.Getenv("IGNORE_COLUMNS"), ","),
		WorkerCount:     1,
		BatchSize:       100000,
	}

	// Validate only required fields
	if cfg.OracleConnStr == "" {
		return nil, fmt.Errorf("ORACLE_CONN_STR is required")
	}
	if cfg.PostgresConnStr == "" {
		return nil, fmt.Errorf("POSTGRES_CONN_STR is required")
	}
	if cfg.TableOracle == "" {
		return nil, fmt.Errorf("TABLE_ORACLE is required")
	}
	if cfg.TablePostgres == "" {
		return nil, fmt.Errorf("TABLE_POSTGRES is required")
	}
	if len(cfg.KeyColumns) == 0 || cfg.KeyColumns[0] == "" {
		return nil, fmt.Errorf("KEY_COLUMNS is required")
	}

	// Remove empty strings from slices
	cfg.KeyColumns = removeEmptyStrings(cfg.KeyColumns)
	cfg.CompareColumns = removeEmptyStrings(cfg.CompareColumns)
	cfg.IgnoreColumns = removeEmptyStrings(cfg.IgnoreColumns)
	cfg.WorkerCount = getIntEnv("WORKER_COUNT", 1)
	cfg.BatchSize = getIntEnv("BATCH_SIZE", 100000)

	// Log configuration
	log.Printf("Loaded configuration:")
	log.Printf("  Oracle Table: %s", cfg.TableOracle)
	log.Printf("  PostgreSQL Table: %s", cfg.TablePostgres)
	if cfg.WhereClause != "" {
		log.Printf("  Where Clause: %s", cfg.WhereClause)
	}
	log.Printf("  Key Columns: %v", cfg.KeyColumns)
	if len(cfg.CompareColumns) > 0 {
		log.Printf("  Compare Columns: %v", cfg.CompareColumns)
	}
	if len(cfg.IgnoreColumns) > 0 {
		log.Printf("  Ignore Columns: %v", cfg.IgnoreColumns)
	}

	return cfg, nil
}

func removeEmptyStrings(slice []string) []string {
	result := make([]string, 0, len(slice))
	for _, s := range slice {
		if s != "" {
			result = append(result, s)
		}
	}
	return result
}

func parseColumns(cols string) []string {
	if cols == "" {
		return nil
	}
	columns := strings.Split(cols, ",")
	result := make([]string, 0, len(columns))
	for _, col := range columns {
		if trimmed := strings.TrimSpace(col); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func getIntEnv(key string, defaultValue int) int {
	if value, err := strconv.Atoi(os.Getenv(key)); err == nil && value > 0 {
		return value
	}
	return defaultValue
}

func (c *Config) validate() error {
	if c.OracleConnStr == "" {
		return fmt.Errorf("ORACLE_CONN_STR is required")
	}
	if c.PostgresConnStr == "" {
		return fmt.Errorf("POSTGRES_CONN_STR is required")
	}
	if c.TableOracle == "" {
		return fmt.Errorf("TABLE_ORACLE is required")
	}
	if c.TablePostgres == "" {
		return fmt.Errorf("TABLE_POSTGRES is required")
	}
	if len(c.KeyColumns) == 0 {
		return fmt.Errorf("KEY_COLUMNS is required")
	}
	return nil
}
