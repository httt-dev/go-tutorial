package database

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/sijms/go-ora/v2"
)

// Record represents a database record
type Record struct {
	KeyValues   map[string]interface{}
	ColValues   map[string]interface{}
	IsDifferent bool
	DiffFields  []string
}

// Database interface defines methods for database operations
type Database interface {
	GetColumns(ctx context.Context, tableName string) ([]string, error)
	CountRecords(ctx context.Context, tableName, whereClause string) (int, error)
	FetchRecords(ctx context.Context, tableName string, columns, keyCols []string, whereClause string, offset, limit int) ([]Record, error)
	Close() error
}

// OracleDB implements Database interface for Oracle
type OracleDB struct {
	db *sql.DB
}

// PostgresDB implements Database interface for PostgreSQL
type PostgresDB struct {
	pool *pgxpool.Pool
}

// NewOracleDB creates a new Oracle database connection
func NewOracleDB(connStr string) (*OracleDB, error) {
	// Add connection options
	connStr += "?prefetch_rows=10000"

	db, err := sql.Open("oracle", connStr)
	if err != nil {
		return nil, fmt.Errorf("error connecting to Oracle: %v", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(50)                  // Increase max connections
	db.SetMaxIdleConns(20)                  // Increase idle connections
	db.SetConnMaxLifetime(time.Hour)        // Maximum lifetime of a connection
	db.SetConnMaxIdleTime(30 * time.Minute) // Maximum idle time of a connection

	// Test the connection with retry
	var pingErr error
	for i := 0; i < 3; i++ {
		pingErr = db.Ping()
		if pingErr == nil {
			break
		}
		time.Sleep(time.Second * time.Duration(i+1))
	}
	if pingErr != nil {
		db.Close()
		return nil, fmt.Errorf("error pinging Oracle database after retries: %v", pingErr)
	}

	log.Printf("Oracle connection pool configured: MaxOpenConns=%d, MaxIdleConns=%d",
		db.Stats().MaxOpenConnections, db.Stats().Idle)
	return &OracleDB{db: db}, nil
}

// NewPostgresDB creates a new PostgreSQL database connection
func NewPostgresDB(ctx context.Context, connStr string) (*PostgresDB, error) {
	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("error parsing PostgreSQL connection string: %v", err)
	}

	// Configure connection pool
	config.MaxConns = 50                      // Increase max connections
	config.MinConns = 10                      // Increase min connections
	config.MaxConnLifetime = time.Hour        // Maximum lifetime of a connection
	config.MaxConnIdleTime = 30 * time.Minute // Maximum idle time of a connection

	// Disable TLS
	config.ConnConfig.TLSConfig = nil

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("error connecting to PostgreSQL: %v", err)
	}

	// Test the connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("error pinging PostgreSQL database: %v", err)
	}

	log.Printf("PostgreSQL connection pool configured: MaxConns=%d, MinConns=%d",
		config.MaxConns, config.MinConns)
	return &PostgresDB{pool: pool}, nil
}

// GetColumns implementation for Oracle
func (o *OracleDB) GetColumns(ctx context.Context, tableName string) ([]string, error) {
	query := fmt.Sprintf("SELECT column_name FROM user_tab_columns WHERE table_name = UPPER('%s') ORDER BY column_id", tableName)
	rows, err := o.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("error querying Oracle columns: %v", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		columns = append(columns, strings.ToLower(col))
	}
	return columns, nil
}

// GetColumns implementation for PostgreSQL
func (p *PostgresDB) GetColumns(ctx context.Context, tableName string) ([]string, error) {
	query := fmt.Sprintf("SELECT column_name FROM information_schema.columns WHERE table_name = '%s'", tableName)
	rows, err := p.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("error querying PostgreSQL columns: %v", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		columns = append(columns, col)
	}
	return columns, nil
}

// retryWithBackoff executes a function with exponential backoff retry
func retryWithBackoff(ctx context.Context, operation func() error) error {
	var err error
	backoff := 100 * time.Millisecond
	maxBackoff := 5 * time.Second

	for i := 0; i < 3; i++ { // Try 3 times
		err = operation()
		if err == nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
	return err
}

// CountRecords implementation for Oracle
func (o *OracleDB) CountRecords(ctx context.Context, tableName, whereClause string) (int, error) {
	if whereClause == "" {
		whereClause = "1=1"
	}
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", tableName, whereClause)
	var count int

	err := retryWithBackoff(ctx, func() error {
		return o.db.QueryRowContext(ctx, query).Scan(&count)
	})
	if err != nil {
		return 0, fmt.Errorf("error counting Oracle records: %v", err)
	}
	return count, nil
}

// CountRecords implementation for PostgreSQL
func (p *PostgresDB) CountRecords(ctx context.Context, tableName, whereClause string) (int, error) {
	if whereClause == "" {
		whereClause = "1=1"
	}
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", tableName, whereClause)
	var count int

	err := retryWithBackoff(ctx, func() error {
		return p.pool.QueryRow(ctx, query).Scan(&count)
	})
	if err != nil {
		return 0, fmt.Errorf("error counting PostgreSQL records: %v", err)
	}
	return count, nil
}

// FetchRecords implementation for Oracle
func (o *OracleDB) FetchRecords(ctx context.Context, tableName string, columns, keyCols []string, whereClause string, offset, limit int) ([]Record, error) {
	startTime := time.Now()
	colStr := strings.Join(columns, ",")

	// Use bind parameters for offset and limit
	query := fmt.Sprintf("SELECT /*+ FIRST_ROWS(%d) */ %s FROM %s", limit, colStr, tableName)

	// Add WHERE clause if provided
	if whereClause != "" {
		query += fmt.Sprintf(" WHERE %s", whereClause)
	}

	// Add ORDER BY and OFFSET/FETCH with bind parameters
	query += fmt.Sprintf(" ORDER BY %s OFFSET :1 ROWS FETCH NEXT :2 ROWS ONLY",
		strings.Join(keyCols, ","))

	var rows *sql.Rows
	var err error

	err = retryWithBackoff(ctx, func() error {
		rows, err = o.db.QueryContext(ctx, query, offset, limit)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("error querying Oracle records: %v", err)
	}
	defer rows.Close()

	records, err := scanRecords(rows, columns, keyCols)
	if err != nil {
		return nil, err
	}

	log.Printf("Oracle fetch completed in %v: %d records, query: %s",
		time.Since(startTime), len(records), query)
	return records, nil
}

// FetchRecords implementation for PostgreSQL
func (p *PostgresDB) FetchRecords(ctx context.Context, tableName string, columns, keyCols []string, whereClause string, offset, limit int) ([]Record, error) {
	startTime := time.Now()
	colStr := strings.Join(columns, ",")

	// Use bind parameters for offset and limit
	query := fmt.Sprintf("SELECT %s FROM %s", colStr, tableName)

	// Add WHERE clause if provided
	if whereClause != "" {
		query += fmt.Sprintf(" WHERE %s", whereClause)
	}

	// Add ORDER BY and OFFSET/LIMIT with bind parameters
	query += fmt.Sprintf(" ORDER BY %s OFFSET $1 LIMIT $2",
		strings.Join(keyCols, ","))

	var rows pgx.Rows
	var err error

	err = retryWithBackoff(ctx, func() error {
		rows, err = p.pool.Query(ctx, query, offset, limit)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("error querying PostgreSQL records: %v", err)
	}
	defer rows.Close()

	records, err := scanRecords(rows, columns, keyCols)
	if err != nil {
		return nil, err
	}

	log.Printf("PostgreSQL fetch completed in %v: %d records, query: %s",
		time.Since(startTime), len(records), query)
	return records, nil
}

// Close implementation for Oracle
func (o *OracleDB) Close() error {
	return o.db.Close()
}

// Close implementation for PostgreSQL
func (p *PostgresDB) Close() error {
	p.pool.Close()
	return nil
}

// scanRecords is a helper function to scan rows into records
func scanRecords(rows interface{}, columns, keyCols []string) ([]Record, error) {
	var records []Record
	var scan func(dest ...interface{}) error
	var next func() bool

	switch r := rows.(type) {
	case *sql.Rows:
		scan = r.Scan
		next = r.Next
	case pgx.Rows:
		scan = r.Scan
		next = r.Next
	default:
		return nil, fmt.Errorf("unsupported rows type")
	}

	for next() {
		cols := make([]interface{}, len(columns))
		colPtrs := make([]interface{}, len(columns))
		for i := range cols {
			colPtrs[i] = &cols[i]
		}

		if err := scan(colPtrs...); err != nil {
			return nil, err
		}

		record := Record{
			KeyValues: make(map[string]interface{}),
			ColValues: make(map[string]interface{}),
		}

		for i, col := range columns {
			normalizedCol := strings.ToLower(col)
			val := cols[i]

			// Only trim spaces for string values
			if val != nil {
				switch v := val.(type) {
				case string:
					val = strings.TrimSpace(v)
				}
			}

			if contains(keyCols, col) {
				record.KeyValues[normalizedCol] = val
			}
			record.ColValues[normalizedCol] = val
		}
		records = append(records, record)
	}
	return records, nil
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if strings.EqualFold(s, item) {
			return true
		}
	}
	return false
}
