package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

// Config holds all configuration
type Config struct {
	SrcOracleDSN   string
	DstPostgresDSN string
	ExpectedDBName string
	TableNames     string
	PartitionBy    string
	PartitionCount int
	FilterWhere    string
}

// LoadConfig loads configuration from environment variables
func LoadConfig() (*Config, error) {
	if err := godotenv.Load(); err != nil {
		fmt.Printf("Could not find .env file, using system environment variables: %v\n", err)
	}

	config := &Config{
		SrcOracleDSN:   buildOracleDSN(),
		DstPostgresDSN: buildPostgresDSN(),
		ExpectedDBName: os.Getenv("DESTINATION_DB_NAME"),
		TableNames:     os.Getenv("TABLE_NAME"),
		PartitionBy:    os.Getenv("PARTITION_BY"),
		FilterWhere:    os.Getenv("FILTER"),
	}

	if config.PartitionBy == "" {
		config.PartitionBy = "rownum"
	}

	if strings.TrimSpace(config.FilterWhere) == "" {
		config.FilterWhere = " AND 1=1 "
	} else {
		config.FilterWhere = " AND " + config.FilterWhere
	}

	if s := os.Getenv("PARTITION_COUNT"); s != "" {
		if cnt, err := strconv.Atoi(s); err == nil && cnt > 0 {
			config.PartitionCount = cnt
		}
	}

	return config, nil
}

// buildOracleDSN builds the Oracle DSN string from environment variables
func buildOracleDSN() string {
	oracleUsername := os.Getenv("SRC_ORACLE_USERNAME")
	oraclePassword := os.Getenv("SRC_ORACLE_PASSWORD")
	oracleHost := os.Getenv("SRC_ORACLE_HOST")
	oraclePort := os.Getenv("SRC_ORACLE_PORT")
	oracleDatabase := os.Getenv("SRC_ORACLE_DATABASE")
	oraclePrefetchRows := os.Getenv("SRC_ORACLE_PREFETCH_ROWS")

	dsn := fmt.Sprintf("oracle://%s:%s@%s:%s/%s", oracleUsername, oraclePassword, oracleHost, oraclePort, oracleDatabase)

	if oraclePrefetchRows != "" {
		dsn = fmt.Sprintf("%s?PREFETCH_ROWS=%s", dsn, oraclePrefetchRows)
	}

	return dsn
}

// buildPostgresDSN builds the PostgreSQL DSN string from environment variables
func buildPostgresDSN() string {
	postgresUsername := os.Getenv("DST_POSTGRES_USERNAME")
	postgresPassword := os.Getenv("DST_POSTGRES_PASSWORD")
	postgresHost := os.Getenv("DST_POSTGRES_HOST")
	postgresPort := os.Getenv("DST_POSTGRES_PORT")
	postgresDatabase := os.Getenv("DST_POSTGRES_DATABASE")

	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s", postgresUsername, postgresPassword, postgresHost, postgresPort, postgresDatabase)
} 