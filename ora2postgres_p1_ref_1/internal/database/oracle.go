package database

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"ora2postgres_p1/internal/models"

	_ "github.com/sijms/go-ora/v2"
)

// OracleDB represents an Oracle database connection
type OracleDB struct {
	*sql.DB
}

// NewOracleDB creates a new Oracle database connection
func NewOracleDB(dsn string) (*OracleDB, error) {
	db, err := sql.Open("oracle", dsn)
	if err != nil {
		return nil, fmt.Errorf("error connecting to Oracle: %w", err)
	}

	// Set pooling options
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(60 * time.Second)

	// Test the connection
	if err = db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("error ping connection: %w", err)
	}

	log.Println("Successfully connected to Oracle.")
	return &OracleDB{db}, nil
}

// GetTableColumns gets column information from a table
func (db *OracleDB) GetTableColumns(tableName string) ([]models.ColumnInfo, error) {
	query := `
		SELECT column_name, data_type
		FROM user_tab_columns
		WHERE table_name = :1
		ORDER BY column_id`
	rows, err := db.Query(query, strings.ToUpper(tableName))
	if err != nil {
		return nil, fmt.Errorf("error querying columns: %w", err)
	}
	defer rows.Close()

	var columns []models.ColumnInfo
	for rows.Next() {
		var col models.ColumnInfo
		if err := rows.Scan(&col.Name, &col.DataType); err != nil {
			return nil, fmt.Errorf("error scanning column info: %w", err)
		}
		columns = append(columns, col)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating column info: %w", err)
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("no columns found for table %s", tableName)
	}
	return columns, nil
}

// HasClobColumn checks if the specified table has a CLOB column
func (db *OracleDB) HasClobColumn(tableName string) (bool, error) {
	query := `
        SELECT COUNT(*)
        FROM user_tab_columns
        WHERE table_name = :1 AND data_type = 'CLOB'
    `
	var count int
	err := db.QueryRow(query, strings.ToUpper(tableName)).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("error checking CLOB column: %w", err)
	}
	return count > 0, nil
}

// GetRowCount gets the total number of rows in a table
func (db *OracleDB) GetRowCount(tableName, filterWhere string) (int64, error) {
	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE 1=1 %s", tableName, filterWhere)
	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("error getting row count: %w", err)
	}
	return count, nil
} 