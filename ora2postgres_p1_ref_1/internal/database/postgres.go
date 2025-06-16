package database

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresDB represents a PostgreSQL database connection
type PostgresDB struct {
	*pgxpool.Pool
}

// NewPostgresDB creates a new PostgreSQL database connection
func NewPostgresDB(dsn string) (*PostgresDB, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse postgres config: %w", err)
	}

	config.MaxConns = 100
	config.MinConns = 5
	config.MaxConnLifetime = time.Hour
	config.MaxConnIdleTime = 30 * time.Minute
	config.HealthCheckPeriod = 1 * time.Minute

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres pool: %w", err)
	}

	// Test the connection
	if err = pool.Ping(context.Background()); err != nil {
		pool.Close()
		return nil, fmt.Errorf("error ping connection: %w", err)
	}

	log.Println("Successfully connected to Postgres.")
	return &PostgresDB{pool}, nil
}

// VerifyConnection verifies the connection to the PostgreSQL database
func (db *PostgresDB) VerifyConnection() (string, error) {
	ctx := context.Background()
	var dbName string

	query := "SELECT current_database()"
	err := db.QueryRow(ctx, query).Scan(&dbName)
	if err != nil {
		return "", fmt.Errorf("error verifying connection: %w", err)
	}

	return dbName, nil
}

// DisableLogging disables triggers on the specified table
func (db *PostgresDB) DisableLogging(tableName string) error {
	actualTableName := extractTableName(tableName)
	ctx := context.Background()
	query := fmt.Sprintf("ALTER TABLE %s DISABLE TRIGGER ALL", actualTableName)
	_, err := db.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to disable triggers for table %s: %w", actualTableName, err)
	}

	log.Printf("Disabled triggers for table: %s", actualTableName)
	return nil
}

// EnableLogging enables triggers on the specified table
func (db *PostgresDB) EnableLogging(tableName string) error {
	actualTableName := extractTableName(tableName)
	ctx := context.Background()
	query := fmt.Sprintf("ALTER TABLE %s ENABLE TRIGGER ALL", actualTableName)
	_, err := db.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to enable triggers for table %s: %w", actualTableName, err)
	}

	log.Printf("Enabled triggers for table: %s", actualTableName)
	return nil
}

// SetSessionRepliRole sets the session_replication_role
func (db *PostgresDB) SetSessionRepliRole(value string) error {
	ctx := context.Background()
	query := fmt.Sprintf("SET session_replication_role = %s", value)
	_, err := db.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to set value for session_replication_role: %s.%w", value, err)
	}
	log.Printf("SET session_replication_role = %s", value)
	return nil
}

// InsertBatch inserts a batch of rows into a table
func (db *PostgresDB) InsertBatch(tableName string, columns []string, batch [][]interface{}) error {
	startTime := time.Now()
	ctx := context.Background()

	// Create temp CSV file
	timeNow := time.Now().UnixNano()
	tempFile := fmt.Sprintf("D:\\pg_backup\\%s_%d.csv", tableName, timeNow)
	tempFileLinux := fmt.Sprintf("/pg_backup/%s_%d.csv", tableName, timeNow)
	file, err := os.Create(tempFile)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tempFile)
	defer file.Close()

	// Write data to CSV
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	if err := writer.Write(columns); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Write data rows
	for _, row := range batch {
		strRow := make([]string, len(row))
		for i, v := range row {
			if v == nil {
				strRow[i] = "\\N" // NULL value in PostgreSQL COPY
			} else {
				switch t := v.(type) {
				case time.Time:
					// Format timestamp to PostgreSQL format
					strRow[i] = t.Format("2006-01-02 15:04:05.000000")
				default:
					strRow[i] = fmt.Sprintf("%v", v)
				}
			}
		}
		if err := writer.Write(strRow); err != nil {
			return fmt.Errorf("failed to write row: %w", err)
		}
	}

	// Flush and close CSV file
	writer.Flush()
	file.Close()

	// Use COPY command to load data
	copyStart := time.Now()
	_, err = db.Exec(ctx, fmt.Sprintf("COPY %s FROM '%s' WITH (FORMAT csv, HEADER true, NULL '\\N')", tableName, tempFileLinux))
	if err != nil {
		return fmt.Errorf("copy to Postgres error: %w", err)
	}

	log.Printf("Inserted batch of %d rows into %s in %v (CSV write: %v, COPY: %v)", 
		len(batch), tableName, time.Since(startTime), 
		copyStart.Sub(startTime), time.Since(copyStart))
	return nil
}

// InsertBatchUseString inserts a batch of rows using string-based query
func (db *PostgresDB) InsertBatchUseString(tableName string, columns []string, batch [][]interface{}) error {
	ctx := context.Background()

	if len(batch) == 0 {
		return nil
	}

	var (
		valueStrings []string
		values       []interface{}
		argCounter   = 1
		numCols      = len(columns)
	)

	for _, row := range batch {
		if len(row) != numCols {
			return fmt.Errorf("invalid number of columns in row: expected %d, got %d", numCols, len(row))
		}
		placeholders := make([]string, numCols)
		for i := range row {
			placeholders[i] = fmt.Sprintf("$%d", argCounter)
			argCounter++
		}
		valueStrings = append(valueStrings, "("+strings.Join(placeholders, ", ")+")")
		values = append(values, row...)
	}

	query := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES %s`,
		tableName,
		strings.Join(columns, ", "),
		strings.Join(valueStrings, ", "),
	)

	_, err := db.Exec(ctx, query, values...)
	if err != nil {
		return fmt.Errorf("insert batch error: %w", err)
	}

	return nil
}

// extractTableName extracts the table name from the input string
func extractTableName(table string) string {
	parts := strings.Split(table, " ")
	if len(parts) > 0 {
		return strings.TrimSpace(parts[0])
	}
	return ""
}

// OptimizeSessionForBulkInsert optimizes PostgreSQL session parameters for bulk insert
func (p *PostgresDB) OptimizeSessionForBulkInsert() error {
	ctx := context.Background()
	// List of parameters to optimize
	optimizations := []string{
		// "SET maintenance_work_mem = '4GB'",
		"SET synchronous_commit = off",
		"SET fsync = off",
		"SET full_page_writes = off",
		// "SET max_wal_size = '16GB'",
		// "SET checkpoint_timeout = '15min'",
		// "SET random_page_cost = 1.1",
		// "SET effective_cache_size = '8GB'",
		// "SET temp_buffers = '1GB'",
		// "SET temp_file_limit = '10GB'",
		// "SET log_min_duration_statement = 1000",
		// "SET log_checkpoints = on",
		// "SET log_connections = on",
		// "SET log_disconnections = on",
		// "SET log_lock_waits = on",
		// "SET log_temp_files = 0",
		// "SET log_autovacuum_min_duration = 0",
		// "SET log_error_verbosity = verbose",
		// "SET client_min_messages = debug1",
		// "SET log_min_messages = debug1",
	}

	// Execute each optimization
	for _, opt := range optimizations {
		if _, err := p.Exec(ctx, opt); err != nil {
			return fmt.Errorf("error setting %s: %v", opt, err)
		}
	}

	log.Println("PostgreSQL session parameters optimized for bulk insert")
	return nil
}

// ResetSessionParameters resets PostgreSQL session parameters to safe values
func (p *PostgresDB) ResetSessionParameters() error {
	ctx := context.Background()
	// List of parameters to reset
	resets := []string{
		// "SET synchronous_commit = on",
		// "SET fsync = on",
		// "SET full_page_writes = on",
		// "SET autovacuum = on",
	}

	// Execute each reset
	for _, reset := range resets {
		if _, err := p.Exec(ctx, reset); err != nil {
			return fmt.Errorf("error resetting %s: %v", reset, err)
		}
	}

	log.Println("PostgreSQL session parameters reset to safe values")
	return nil
} 