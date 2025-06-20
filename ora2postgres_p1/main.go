package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/transform"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	_ "github.com/sijms/go-ora/v2"
)

var (
	expectedDBName string
	srcOracleDSN   string
	dstPostgresDSN string
)

type TableMapping struct {
	SrcTable string
	DstTable string
}

type ColumnInfo struct {
	Name     string
	DataType string
}

type TableSummary struct {
	SourceTable      string
	SourceRowCount   int64
	DestinationTable string
	CopiedRowCount   int64
	Status           string
}

var summaryList []TableSummary

const (
	CHAN_QUEUE      = 50_000 // 50_000
	BATCH_SIZE      = 50_000 // 50_000
	LOG_READED_ROWS = 100_000

	CHAN_QUEUE_HAS_CLOB      = 50
	BATCH_SIZE_HAS_CLOB      = 50
	LOG_READED_HAS_CLOB_ROWS = 50
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Printf("Could not find .env file, using system environment variables: %v", err)
	}
}

// Function to set up logging
func setupLogging() {
	// Create the logs directory if it doesn't exist
	logDir := "logs"
	if _, err := os.Stat(logDir); os.IsNotExist(err) {
		err := os.Mkdir(logDir, 0755)
		if err != nil {
			log.Fatalf("Unable to create logs directory: %v", err)
		}
	}

	// Get the current time to name the log file
	now := time.Now()
	logFileName := fmt.Sprintf("%s/log_%s.log", logDir, now.Format("20060102_150405"))

	// Open the log file
	logFile, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Unable to open log file: %v", err)
	}

	// Write logs to both console and file
	multiWriter := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(multiWriter)

	log.Printf("Logging started. Logs are being written to %s", logFileName)
}

func insertBatch(pool *pgxpool.Pool, tableName string, columns []string, batch [][]interface{}) error {

	ctx := context.Background()

	copyData := make([][]interface{}, len(batch))
	// for i, row := range batch {
	// 	copyData[i] = row
	// }
	copy(copyData, batch)

	// copy data to PostgreSQL
	_, err := pool.CopyFrom(
		ctx,
		pgx.Identifier{tableName},
		columns,
		pgx.CopyFromRows(copyData),
	)

	if err != nil {
		return fmt.Errorf("copy to Postgres error: %w", err)
	}

	return nil
}

func insertBatchUseString(pool *pgxpool.Pool, tableName string, columns []string, batch [][]interface{}) error {
	ctx := context.Background()

	if len(batch) == 0 {
		return nil
	}

	// make placeholders ($1, $2), ($3, $4), ...
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

	_, err := pool.Exec(ctx, query, values...)
	if err != nil {
		return fmt.Errorf("insert batch error: %w", err)
	}

	return nil
}

// insertBatchWithFallback
func insertBatchWithFallback(db *pgxpool.Pool, tableName string, columns []string, columnTypes []interface{}, batch [][]interface{}) error {

	err := insertBatch(db, tableName, columns, batch)

	if err == nil {
		return nil
	} else {
		return err
	}
	// // If the batch contains only one record, log it and return the error
	// if len(batch) == 1 {
	//     log.Printf("Error record: %v\n", batch[0])

	//     err = insertBatchUseString(db, tableName, columns, batch)
	//     if err == nil {
	//         return nil
	//     }
	//     log.Printf("retry insert error record: %v\n", err)
	//     return err
	// }
	// // Split the batch into two parts and try inserting each part
	// mid := len(batch) / 2
	// err1 := insertBatchWithFallback(db, tableName, columns, columnTypes, batch[:mid])
	// err2 := insertBatchWithFallback(db, tableName, columns, columnTypes, batch[mid:])
	// if err1 != nil || err2 != nil {
	//     return fmt.Errorf("error inserting split batch: err1: %v, err2: %v", err1, err2)
	// }
	// return nil
}

// worker function to process data from the channel and insert it into PostgreSQL
// It takes the worker ID, database connection, destination table name, columns, column types,
// data channel, error channel, and wait group as parameters
// The function reads data from the data channel, processes it, and inserts it into PostgreSQL
// It also handles errors and logs the progress
func worker(ctx context.Context, workerId int, batchSize int, db *pgxpool.Pool, dstTable string, columns []string, columnTypes []interface{}, dataChan <-chan []interface{}, errorChan chan<- error, wg *sync.WaitGroup, copiedRowCount *int64) {
	defer wg.Done()
	// batchSize := BATCH_SIZE // Number of rows per batch (adjustable)
	batch := make([][]interface{}, 0, batchSize)

	actualDBName, err := verifyConnection(db)
	if err != nil {
		errorChan <- fmt.Errorf("Worker %d: failed to verify connection: %v", workerId, err)
		return
	}
	if actualDBName != expectedDBName {
		errorChan <- fmt.Errorf("Worker %d: wrong database connected! Expected: %s, Actual: %s", workerId, expectedDBName, actualDBName)
		return
	}
	log.Printf("Worker %d: connected to the correct database: %s", workerId, actualDBName)

	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %d: received cancellation signal, stopping...", workerId)
			return
		case row, ok := <-dataChan:
			if !ok {
				// Channel closed, process remaining batch
				if len(batch) > 0 {
					if err := insertBatchWithFallback(db, dstTable, columns, columnTypes, batch); err != nil {
						errorChan <- fmt.Errorf("worker %d: error inserting last batch: %v", workerId, err)
						return // Exit immediately if the last batch fails
					}
					atomic.AddInt64(copiedRowCount, int64(len(batch))) // Update the count
				}
				log.Printf("Worker %d: finished processing remaining rows", workerId)
				return
			}
			batch = append(batch, row)
			if len(batch) >= batchSize {
				if err := insertBatchWithFallback(db, dstTable, columns, columnTypes, batch); err != nil {
					errorChan <- fmt.Errorf("worker %d: error inserting batch: %v", workerId, err)
				} else {
					atomic.AddInt64(copiedRowCount, int64(len(batch))) // Update the count
				}
				batch = batch[:0]
			}

		}
	}

}

// createOraclePool creates a connection pool for Oracle using sijms/go-ora
// It sets various connection pool parameters such as max connections, min connections, etc.
func createOraclePool(dsn string) (*sql.DB, error) {
	// Extract host and service information from the DSN
	host, service := extractHostAndService(dsn)
	log.Printf("Connecting to Oracle at host: %s, service: %s", host, service)

	// Open a connection to the Oracle database using sijms/go-ora
	db, err := sql.Open("oracle", dsn)
	if err != nil {
		return nil, fmt.Errorf("error connecting to Oracle: %w", err)
	}

	// Set pooling options
	// db.SetConnMaxIdleTime(30 * time.Second) // Maximum idle connection time
	db.SetMaxOpenConns(100)                  // Maximum number of open connections
	db.SetMaxIdleConns(5)                   // Maximum number of idle connections
	db.SetConnMaxLifetime(60 * time.Second) // Maximum connection lifetime

	// Test the connection
	if err = db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("error ping connection: %w", err)
	}

	log.Println("Successfully connected to Oracle.")
	return db, nil
}

// createPostgresPool creates a connection pool for PostgreSQL using pgxpool
// It sets various connection pool parameters such as max connections, min connections, etc.
func createPostgresPool(pgDSN string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(pgDSN)
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

	return pool, nil
}

// Helper function to extract host and service from the DSN
// The DSN format is expected to be "user:password@host/service_name"
// If the format is invalid, it returns "unknown" for both host and service.
func extractHostAndService(dsn string) (string, string) {
	parts := strings.Split(dsn, "@")
	if len(parts) < 2 {
		return "unknown", "unknown" // Return default values if DSN format is invalid
	}

	connectionInfo := parts[1] // Extract the part after '@'
	hostAndService := strings.Split(connectionInfo, "/")
	if len(hostAndService) == 2 {
		return hostAndService[0], hostAndService[1] // Return host and service
	}

	return hostAndService[0], "unknown" // If service is missing, return "unknown"
}

// verifyConnection verifies the connection to the PostgreSQL database
// It checks if the connection is valid and retrieves the current database name.
func verifyConnection(db *pgxpool.Pool) (string, error) {
	ctx := context.Background()
	var dbName string

	query := "SELECT current_database()"
	err := db.QueryRow(ctx, query).Scan(&dbName)
	if err != nil {
		return "", fmt.Errorf("error verifying connection: %w", err)
	}

	return dbName, nil
}

// disableLogging disables triggers on the specified table in the PostgreSQL database
// It verifies the connection to the database and checks if the correct database is connected.
func disableLogging(db *pgxpool.Pool, tableName string) error {
	// Verify the connection and ensure the correct database is connected
	actualDBName, err := verifyConnection(db)
	if err != nil {
		return fmt.Errorf("failed to verify connection: %w", err)
	}

	// Check if the connected database matches the expected one
	if actualDBName != expectedDBName {
		return fmt.Errorf("wrong database connected! Expected: %s, Actual: %s", expectedDBName, actualDBName)
	}
	log.Printf("Connected to the correct database: %s", actualDBName)

	// Extract the actual table name from the input
	actualTableName := extractTableName(tableName) // Assume this function extracts the table name

	// Disable triggers on the table
	ctx := context.Background()
	// MUST BE RUN WITH SUPERUSER PRIVILEGES !!!!
	query := fmt.Sprintf("ALTER TABLE %s DISABLE TRIGGER ALL", actualTableName)
	_, err = db.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to disable triggers for table %s: %w", actualTableName, err)
	}

	log.Printf("Disabled triggers for table: %s", actualTableName)
	return nil
}

// enableLogging enables triggers on the specified table in the PostgreSQL database
// It verifies the connection to the database and checks if the correct database is connected.
func enableLogging(db *pgxpool.Pool, tableName string) error {
	// Verify the connection and ensure the correct database is connected
	actualDBName, err := verifyConnection(db)
	if err != nil {
		return fmt.Errorf("failed to verify connection: %w", err)
	}

	// Check if the connected database matches the expected one
	if actualDBName != expectedDBName {
		return fmt.Errorf("wrong database connected! Expected: %s, Actual: %s", expectedDBName, actualDBName)
	}
	log.Printf("Connected to the correct database: %s", actualDBName)

	// Extract the actual table name from the input
	actualTableName := extractTableName(tableName) // Assume this function extracts the table name

	// Enable triggers on the table
	ctx := context.Background()
	query := fmt.Sprintf("ALTER TABLE %s ENABLE TRIGGER ALL", actualTableName)
	_, err = db.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to enable triggers for table %s: %w", actualTableName, err)
	}

	log.Printf("Enabled triggers for table: %s", actualTableName)
	return nil
}
// parseTableMappings parses the table names from the environment variable TABLE_NAME
func parseTableMappings(tableNames string) []TableMapping {
	var mappings []TableMapping
	tables := strings.Split(tableNames, ",")
	for _, table := range tables {
		table = strings.TrimSpace(table)
		if table == "" {
			continue
		}
		parts := strings.Split(table, ":")
		srcTable := strings.TrimSpace(parts[0])
		srcTable = strings.ToUpper(srcTable)  // Convert to uppercase for consistency
		dstTable := strings.ToUpper(srcTable) // Default to source table name
		if len(parts) > 1 {
			dstTable = strings.TrimSpace(parts[1])
		}
		mappings = append(mappings, TableMapping{SrcTable: srcTable, DstTable: dstTable})
	}
	return mappings
}

// extractTableName extracts the table name from the input string
func extractTableName(table string) string {
	parts := strings.Split(table, " ")
	if len(parts) > 0 {
		return strings.TrimSpace(parts[0])
	}
	return ""
}
// printSummary prints a summary of the data migration process
func printSummary() {
	log.Println("=== SUMMARY ===")
	log.Printf("%-40s %-15s %-40s %-15s %-10s\n", "Source Table", "Source Rows", "Destination Table", "Copied Rows", "Status")
	log.Println(strings.Repeat("-", 120))
	for _, summary := range summaryList {
		log.Printf("%-40s %-15d %-40s %-15d %-10s\n",
			summary.SourceTable, summary.SourceRowCount, summary.DestinationTable, summary.CopiedRowCount, summary.Status)
	}
	log.Println(strings.Repeat("-", 120))
}

// Function to get column data types from a table
func getTableColumns(db *sql.DB, tableName string) ([]ColumnInfo, error) {
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

	var columns []ColumnInfo
	for rows.Next() {
		var col ColumnInfo
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

// maybeDecodeShiftJIS checks if the string is valid UTF-8 and decodes it from Shift-JIS if not
func maybeDecodeShiftJIS(s string) string {
	if utf8.ValidString(s) {
		return s // already valid UTF-8
	}
	decoded, err := decodeShiftJIS(s)
	if err != nil {
		return s // fallback to original string if decoding fails
	}
	return decoded
}

// decodeShiftJIS decodes a Shift-JIS encoded string to UTF-8
func decodeShiftJIS(input string) (string, error) {
	reader := transform.NewReader(strings.NewReader(input), japanese.ShiftJIS.NewDecoder())
	decoded, err := io.ReadAll(reader)
	if err != nil {
		return "", err
	}
	return string(decoded), nil
}
// hasClobColumn checks if the specified table has a CLOB column
func hasClobColumn(db *sql.DB, tableName string) (bool, error) {
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

// BuildOracleDSN builds the Oracle DSN string from environment variables.
func buildOracleDSN() string {
	oracleUsername := os.Getenv("SRC_ORACLE_USERNAME")
	oraclePassword := os.Getenv("SRC_ORACLE_PASSWORD")
	oracleHost := os.Getenv("SRC_ORACLE_HOST")
	oraclePort := os.Getenv("SRC_ORACLE_PORT")
	oracleDatabase := os.Getenv("SRC_ORACLE_DATABASE")
	oraclePrefetchRows := os.Getenv("SRC_ORACLE_PREFETCH_ROWS")

	// Construct the base DSN
	dsn := fmt.Sprintf("oracle://%s:%s@%s:%s/%s", oracleUsername, oraclePassword, oracleHost, oraclePort, oracleDatabase)

	// Add query parameters if provided
	if oraclePrefetchRows != "" {
		dsn = fmt.Sprintf("%s?PREFETCH_ROWS=%s", dsn, oraclePrefetchRows)
	}

	return dsn
}

// buildGodrorDSN builds the Oracle DSN string for the godror library from environment variables.
func buildGodrorDSN() string {
	oracleUsername := os.Getenv("SRC_ORACLE_USERNAME")
	oraclePassword := os.Getenv("SRC_ORACLE_PASSWORD")
	oracleHost := os.Getenv("SRC_ORACLE_HOST")
	oraclePort := os.Getenv("SRC_ORACLE_PORT")
	oracleDatabase := os.Getenv("SRC_ORACLE_DATABASE")

	// Construct the DSN in the format: username/password@host:port/database
	dsn := fmt.Sprintf("%s/%s@%s:%s/%s", oracleUsername, oraclePassword, oracleHost, oraclePort, oracleDatabase)
	return dsn
}

// BuildPostgresDSN builds the PostgreSQL DSN string from environment variables.
func buildPostgresDSN() string {
	postgresUsername := os.Getenv("DST_POSTGRES_USERNAME")
	postgresPassword := os.Getenv("DST_POSTGRES_PASSWORD")
	postgresHost := os.Getenv("DST_POSTGRES_HOST")
	postgresPort := os.Getenv("DST_POSTGRES_PORT")
	postgresDatabase := os.Getenv("DST_POSTGRES_DATABASE")

	// Construct the DSN
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", postgresUsername, postgresPassword, postgresHost, postgresPort, postgresDatabase)
	return dsn
}

// createOraclePoolUseGodrorDriver creates a connection pool for Oracle using the godror driver
// must be instal oracle client and set environment variable ORACLE_HOME
// func createOraclePoolUseGodrorDriver(dsn string, timezone string) (*sql.DB, error) {
// 	// parsing DSN
// 	params, err := godror.ParseDSN(dsn)
// 	if err != nil {
// 		return nil, fmt.Errorf("error when parsing DSN: %w", err)
// 	}

// 	// convert timezone to *time.Location
// 	loc, err := time.LoadLocation(timezone)
// 	if err != nil {
// 		return nil, fmt.Errorf("error load timezone %s: %w", timezone, err)
// 	}
// 	params.Timezone = loc

// 	// Set pooling options (if needed)
// 	params.SessionTimeout = 60 * time.Second
// 	params.WaitTimeout = 30 * time.Second
// 	params.MaxSessions = 20
// 	params.MinSessions = 5
// 	params.SessionIncrement = 2
// 	params.Charset = "UTF-8"

// 	// Set pooling options (if needed)
// 	db, err := sql.Open("godror", params.StringWithPassword())
// 	if err != nil {
// 		return nil, fmt.Errorf("error connecting to Oracle: %w", err)
// 	}

// 	// Test the connection
// 	if err = db.Ping(); err != nil {
// 		db.Close()
// 		return nil, fmt.Errorf("Successfully connected to Oracle. %w", err)
// 	}

// 	return db, nil
// }

// setSessionRepliRole sets the session_replication_role in PostgreSQL
func setSessionRepliRole(db *pgxpool.Pool, value string) error {
	// Enable triggers on the table
	ctx := context.Background()
	query := fmt.Sprintf("SET session_replication_role = %s", value)
	_, err := db.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to set value for session_replication_role: %s.%w", value, err)
	}
	log.Printf("SET session_replication_role = %s", value)
	return nil
}

// main is the entry point of the application
func main() {
	// Set up logging
	setupLogging()

	startTime := time.Now()
	defer func() {
		// Print summary
		printSummary()

		log.Printf("Total execution time: %v", time.Since(startTime))
	}()

	// Retrieve required environment variables
	srcOracleDSN = buildOracleDSN()
	fmt.Println("Oracle DSN:", srcOracleDSN)

	dstPostgresDSN = buildPostgresDSN()
	fmt.Println("Postgres DSN:", dstPostgresDSN)

	expectedDBName = os.Getenv("DESTINATION_DB_NAME")

	if srcOracleDSN == "" || dstPostgresDSN == "" || expectedDBName == "" {
		log.Fatal("Please set the environment variables SRC_ORACLE_DSN, DST_POSTGRES_DSN, DESTINATION_DB_NAME")
	}

	tableNames := os.Getenv("TABLE_NAME")
	if tableNames == "" {
		log.Fatal("Please set the environment variable TABLE_NAME")
	}

	tableMappings := parseTableMappings(tableNames)
	tbl, err := json.MarshalIndent(tableMappings, "", "  ")
	if err != nil {
		log.Fatalf("Error parsing table mapping: %v", err)
	}
	log.Printf("List of tables to copy:\n%v\n", string(tbl))

	partitionBy := os.Getenv("PARTITION_BY")
	if partitionBy == "" {
		partitionBy = "rownum"
	}
	partitionColumn := os.Getenv("PARTITION_COLUMN")
	filterWhere := os.Getenv("FILTER")
	if strings.TrimSpace(filterWhere) == "" {
		filterWhere = " AND 1=1 "
	} else {
		filterWhere = " AND " + filterWhere
	}

	// Connect to Source Oracle
	srcOracleDB, err := createOraclePool(srcOracleDSN)
	if err != nil {
		log.Fatalf("Error creating SOURCE Oracle pool: %v", err)
	}
	defer srcOracleDB.Close()

	// Connect to Destination Postgres
	dstPostgresDB, err := createPostgresPool(dstPostgresDSN)
	if err != nil {
		log.Fatalf("Error creating PostgreSQL pool: %v", err)
	}
	defer dstPostgresDB.Close()

	// Set session_replication_role to replica
	if err := setSessionRepliRole(dstPostgresDB, "replica"); err != nil {
	    log.Fatalf("Error setting session_replication_role to replica: %v", err)
	}
	defer func() {
	    if err := setSessionRepliRole(dstPostgresDB, "origin"); err != nil {
	        log.Printf("Error setting session_replication_role back to origin: %v", err)
	    }
	}()

	for _, mapping := range tableMappings {
		srcTable := extractTableName(mapping.SrcTable)
		dstTable := strings.ToLower(extractTableName(mapping.DstTable))

		log.Printf(strings.Repeat("-", 20)+"Starting to process table: source=%s, destination=%s"+strings.Repeat("-", 20), srcTable, dstTable)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Disable logging before starting
		err := disableLogging(dstPostgresDB, dstTable)
		if err != nil {
			log.Printf("\033[31m[ERROR] Error disabling logging for table %s: %v\033[0m", dstTable, err)
			continue
		}
		// SET session_replication_role = replica;

		defer func(dstTable string) {
			if err := enableLogging(dstPostgresDB, dstTable); err != nil {
				log.Printf("\033[31m[ERROR] Error enabling logging for table %s: %v\033[0m", dstTable, err)
			}
		}(dstTable)

		// Perform the data migration process for the current table
		err = migrateTable(ctx, cancel, srcOracleDB, dstPostgresDB, srcTable, dstTable, partitionBy, partitionColumn, filterWhere)
		if err != nil {
			log.Printf("\033[31m[ERROR] Error processing table source=%s, destination=%s: %v\033[0m", srcTable, dstTable, err)
			continue
		}
		log.Printf("Finished processing table: source=%s, destination=%s", srcTable, dstTable)
	}

	log.Printf("Data migration completed.")
}

// migrateTable migrates data from the source Oracle table to the destination PostgreSQL table
func migrateTable(ctx context.Context, cancel context.CancelFunc, srcDB *sql.DB, dstDB *pgxpool.Pool, srcTable string, dstTable string, partitionBy string, partitionColumn string, filterWhere string) error {
	var sourceRowCount, copiedRowCount int64
	var migrationError error

	chanQueue := CHAN_QUEUE
	batchSize := BATCH_SIZE
	logReadedRows := LOG_READED_ROWS

	hasClob, err := hasClobColumn(srcDB, srcTable)
	if err != nil {
		log.Fatalf("\033[31m[ERROR] Error checking CLOB column: %v\033[0m", err)
	}

	if hasClob {
		chanQueue = CHAN_QUEUE_HAS_CLOB
		batchSize = BATCH_SIZE_HAS_CLOB
		logReadedRows = LOG_READED_HAS_CLOB_ROWS

		log.Println("CLOB column detected, adjusting batch size and channel queue")
	}

	log.Printf("Starting migration for table: %s to %s", srcTable, dstTable)

	// Get column information from Oracle
	columnsInfo, err := getTableColumns(srcDB, srcTable)
	if err != nil {
		return fmt.Errorf("error getting columns for %s: %w", srcTable, err)
	}

	columns := make([]string, len(columnsInfo))
	for i, col := range columnsInfo {
		columns[i] = col.Name
	}

	for i, col := range columns {
		columns[i] = strings.ToLower(col)
	}
	// log.Printf("Columns (convert to lower): %v", columns)

	// Initialize worker pool for Oracle
	numWorkers := 10
	if s := os.Getenv("PARTITION_COUNT"); s != "" {
		if cnt, err := strconv.Atoi(s); err == nil && cnt > 0 {
			numWorkers = cnt
		}
	}
	partitionCount := numWorkers
	// Create a channel to transmit rows of data
	dataChan := make(chan []interface{}, chanQueue)

	// Create a channel to receive errors from workers
	errorChan := make(chan error, numWorkers)

	// Goroutine to handle errors from errorChan
	go func() {
		for err := range errorChan {
			if migrationError == nil {
				migrationError = err
			} else {
				log.Printf("Additional error: %v", err)
			}
			cancel() // Cancel all workers
		}
	}()

	extractTableName := extractTableName(dstTable)
	var workerWg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		workerWg.Add(1)
		// go worker(i, dstDB, extractTableName, columns, nil, dataChan, &workerWg)
		go worker(ctx, i, batchSize, dstDB, extractTableName, columns, nil, dataChan, errorChan, &workerWg, &copiedRowCount)
	}

	// Process data from Oracle in partitioned mode

	if strings.ToLower(partitionBy) == "rownum" {
		var totalRows int64
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE 1=1 %s", srcTable, filterWhere)
		if err := srcDB.QueryRow(countQuery).Scan(&totalRows); err != nil {
			log.Fatalf("Error retrieving total rows %v from query: %v", err, countQuery)
			migrationError = fmt.Errorf("Error retrieving total rows: %v", err)
		}
		log.Printf("Total rows: %s", totalRows)

		sourceRowCount = totalRows
		partitionCount = numWorkers

		rangeSize := totalRows / int64(partitionCount)
		if totalRows%int64(partitionCount) != 0 {
			rangeSize++
		}

		colList := strings.Join(columns, ", ")
		var partitionWg sync.WaitGroup
		for i := 0; i < partitionCount; i++ {
			startRow := int64(i)*rangeSize + 1
			endRow := startRow + rangeSize - 1
			if endRow > totalRows {
				endRow = totalRows
			}
			partitionWg.Add(1)
			go func(startRow, endRow int64, partitionId int) {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("Partition %d: Panic occurred! Error: %v", partitionId, r)
					}
					log.Printf("Partition %d: done", partitionId)
					partitionWg.Done()
				}()

				if startRow > endRow {
					log.Printf("Partition %d: No data to process (startRow: %d, endRow: %d)", partitionId, startRow, endRow)
					return
				}

				query := fmt.Sprintf("SELECT %s FROM (SELECT t.*, rownum rnum FROM %s t WHERE 1=1 %s) WHERE rnum BETWEEN %d AND %d", colList, srcTable, filterWhere, startRow, endRow)
				if partitionCount <= 1 {
					query = fmt.Sprintf("SELECT %s FROM %s WHERE 1=1 %s", colList, srcTable, filterWhere)
				}
				log.Printf("Query %s: ", query)

				t := time.Now()

				select {
				case <-ctx.Done():
					log.Printf("Partition %d: Received cancellation signal, stopping...", partitionId)
					return
				default:
					// continue if no cancellation signal
				}

				rows, err := srcDB.Query(query)
				if err != nil {
					log.Printf("Partition %d: query error: %v", partitionId, err)
					migrationError = fmt.Errorf("Partition %d: query error: %v", partitionId, err)
					return
				}
				log.Printf("Finish query with partitionId %d: time : %v", partitionId, time.Now().Sub(t))

				defer rows.Close()

				colCount := len(columns)
				colTypes, _ := rows.ColumnTypes()

				count := 0
				scanArgs := make([]interface{}, colCount)
				// for i := range scanArgs {
				//     scanArgs[i] = new(interface{})
				// }

				for i := 0; i < colCount; i++ {
					dbType := colTypes[i].DatabaseTypeName()
					switch dbType {
					case "VARCHAR2", "VARCHAR", "CHAR", "NCHAR", "NVARCHAR2", "TEXT", "CLOB": //CLOB??
						var s sql.NullString
						scanArgs[i] = &s // use sql.NullString to handle NULL values
					default:
						var raw interface{}
						scanArgs[i] = &raw
					}
				}

				for rows.Next() {
					select {
					case <-ctx.Done():
						log.Printf("Partition %d: Received cancellation signal while processing rows, stopping...", partitionId)
						return
					default:
						// continue if no cancellation signal
					}

					if err := rows.Scan(scanArgs...); err != nil {
						log.Printf("Partition %d: row scan error: %v", partitionId, err)
						migrationError = fmt.Errorf("Partition %d: row scan error: %v", partitionId, err)
						return
					}
					rowData := make([]interface{}, colCount)
					// for i, ptr := range scanArgs {
					//     rowData[i] = *(ptr.(*interface{}))
					// }
					for i, v := range scanArgs {
						switch val := v.(type) {
						case *sql.NullString:
							if val.Valid {
								rowData[i] = maybeDecodeShiftJIS(val.String)
							} else {
								rowData[i] = nil
							}
						default:
							rowData[i] = *(v.(*interface{}))
						}
					}

					dataChan <- rowData
					count++

					if count%logReadedRows == 0 {
						log.Printf("Partition %d: Read %d rows", partitionId, count)
					}
				}

				if err = rows.Err(); err != nil {
					log.Printf("Partition %d: error iterating over rows: %v", partitionId, err)
					migrationError = fmt.Errorf("Partition %d: error iterating over rows: %v", partitionId, err)
				}

				log.Printf("Partition %d: finished reading, total rows: %d", partitionId, count)
			}(startRow, endRow, i)
		}

		partitionWg.Wait()

	} 

	log.Printf("Closing channel for table: %s", srcTable)
	// Close the channel after reading is complete
	close(dataChan)

	// Wait for workers to finish inserting data
	workerWg.Wait()

	// Check for errors from workers
	close(errorChan)

	if migrationError != nil {
		//log.Printf("Error occurred while migrating data from table: %s. Error: %v", srcTable, migrationError)
		cancel() // Cancel all workers
	} else {
		log.Printf("Successfully migrated data from table: %s", srcTable)
	}

	// Add summary to the list
	status := "\033[32m✔ Success\033[0m"
	if migrationError != nil {
		status = "\033[31m✗ Failed\033[0m"
	}
	summaryList = append(summaryList, TableSummary{
		SourceTable:      srcTable,
		SourceRowCount:   sourceRowCount,
		DestinationTable: dstTable,
		CopiedRowCount:   copiedRowCount,
		Status:           status,
	})

	log.Printf("Finished migrating data from table: %s", srcTable)
	return migrationError
}