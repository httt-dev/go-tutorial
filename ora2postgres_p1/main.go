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
	"time"
	"unicode/utf8"

	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/transform"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	_ "github.com/sijms/go-ora/v2"
)

var(
	expectedDBName string
    srcOracleDSN string
    dstPostgresDSN  string
)

type ColumnInfo struct {
	Name     string
	DataType string
}

const (
	CHAN_QUEUE = 50_000  // 50_000
	BATCH_SIZE = 50_000  // 50_000
	LOG_READED_ROWS =  10000 // 10_000
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
    }
    // If the batch contains only one record, log it and return the error
    if len(batch) == 1 {
        log.Printf("Error record: %v\n", batch[0])

        err = insertBatchUseString(db, tableName, columns, batch)
        if err == nil {
            return nil
        }
        log.Printf("retry insert error record: %v\n", err)
        return err
    }
    // Split the batch into two parts and try inserting each part
    mid := len(batch) / 2
    err1 := insertBatchWithFallback(db, tableName, columns, columnTypes, batch[:mid])
    err2 := insertBatchWithFallback(db, tableName, columns, columnTypes, batch[mid:])
    if err1 != nil || err2 != nil {
        return fmt.Errorf("error inserting split batch: err1: %v, err2: %v", err1, err2)
    }
    return nil
}

// worker function to process data from the channel and insert it into PostgreSQL
// It takes the worker ID, database connection, destination table name, columns, column types,
// data channel, error channel, and wait group as parameters
// The function reads data from the data channel, processes it, and inserts it into PostgreSQL
// It also handles errors and logs the progress
func worker(workerId int, db *pgxpool.Pool, dstTable string, columns []string, columnTypes []interface{}, dataChan <-chan []interface{}, errorChan chan<- error, wg *sync.WaitGroup) {
    defer wg.Done()
    batchSize := BATCH_SIZE // Number of rows per batch (adjustable)
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

    for row := range dataChan {
        batch = append(batch, row)
        if len(batch) >= batchSize {
            if err := insertBatchWithFallback(db, dstTable, columns, columnTypes, batch); err != nil {
                errorChan <- fmt.Errorf("Worker %d: error inserting batch: %v", workerId, err)
            }
            batch = batch[:0]
        }
    }
    // Insert remaining data in the batch
    if len(batch) > 0 {
        if err := insertBatchWithFallback(db, dstTable, columns, columnTypes, batch); err != nil {
            errorChan <- fmt.Errorf("Worker %d: error inserting last batch: %v", workerId, err)
        }
    }
    log.Printf("Worker %d: finished processing", workerId)
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

    // Set pooling options (if needed)
    db.SetMaxOpenConns(20)         // Maximum number of open connections
    db.SetMaxIdleConns(5)          // Maximum number of idle connections
    db.SetConnMaxLifetime(60 * time.Second) // Maximum connection lifetime

    // Test the connection
    if err = db.Ping(); err != nil {
        db.Close()
        return nil, fmt.Errorf("error testing connection: %w", err)
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

	config.MaxConns = 20
	config.MinConns = 5
	config.MaxConnLifetime = time.Hour
	config.MaxConnIdleTime = 30 * time.Minute
	config.HealthCheckPeriod = 1 * time.Minute

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres pool: %w", err)
	}

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

// parseTableMappings parses the table mappings from the input string
// The input string should be in the format "src_table:dst_table, src_table2:dst_table2"
// If the destination table is not specified, it defaults to the source table name
// Example: "src_table1:dst_table1, src_table2" will map src_table1 to dst_table1 and src_table2 to src_table2
// The function returns a map where the keys are source table names and the values are destination table names
// If the input string is empty, it returns an empty map
// If the input string is not in the correct format, it returns an error
func parseTableMappings(tableNames string) map[string]string {
    mappings := make(map[string]string)
    tables := strings.Split(tableNames, ",")
    for _, table := range tables {
        table = strings.TrimSpace(table)
        if table == "" {
            continue
        }
        parts := strings.Split(table, ":")
        srcTable := strings.TrimSpace(parts[0])
        dstTable := srcTable 
        if len(parts) > 1 {
            dstTable = strings.TrimSpace(parts[1])
        }
        mappings[srcTable] = dstTable
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

func main() {
    // Set up logging
    setupLogging()

    startTime := time.Now()
    defer func() {
        log.Printf("Total execution time: %v", time.Since(startTime))
    }()

    // Retrieve required environment variables
    srcOracleDSN = os.Getenv("SRC_ORACLE_DSN")
    dstPostgresDSN = os.Getenv("DST_POSTGRES_DSN")
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


	for srcTable, dstTable := range tableMappings {
        dstTable = strings.ToLower(dstTable)
		log.Printf("Starting to process table: source=%s, destination=%s", srcTable, dstTable)
		
		// Disable logging before starting
        err := disableLogging(dstPostgresDB, dstTable)
        if err != nil {
            log.Printf("Error disabling logging for table %s: %v", dstTable, err)
            continue
        }

        defer func(dstTable string) {
            if err := enableLogging(dstPostgresDB, dstTable); err != nil {
                log.Printf("Error enabling logging for table %s: %v", dstTable, err)
            } 
        }(dstTable)

		// Perform the data migration process for the current table
        err = migrateTable(srcOracleDB, dstPostgresDB, srcTable, dstTable, partitionBy, partitionColumn, filterWhere)
        if err != nil {
            log.Printf("Error processing table source=%s, destination=%s: %v", srcTable, dstTable, err)
            continue
        }
        log.Printf("Finished processing table: source=%s, destination=%s", srcTable, dstTable)
	}

    log.Printf("Data migration completed.")
}


func migrateTable(srcDB *sql.DB, dstDB *pgxpool.Pool, srcTable string, dstTable string, partitionBy string, partitionColumn string, filterWhere string) error {

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

	// // Retrieve column names from Oracle
    // dummyQuery := fmt.Sprintf("SELECT * FROM %s WHERE 1=0", srcTable)
    // dummyRows, err := srcDB.Query(dummyQuery)
    // if err != nil {
    //     log.Fatalf("Error executing dummy query: %v", err)
    // }
    // columns, err := dummyRows.Columns()
    // if err != nil {
    //     log.Fatalf("Error getting columns: %v", err)
    // }
    // dummyRows.Close()

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
    // Create a channel to transmit rows of data
    dataChan := make(chan []interface{}, CHAN_QUEUE)

    // Create a channel to receive errors from workers
    errorChan := make(chan error, numWorkers)

    extractTableName := extractTableName(dstTable)
    var workerWg sync.WaitGroup
    for i := 0; i < numWorkers; i++ {
        workerWg.Add(1)
        // go worker(i, dstDB, extractTableName, columns, nil, dataChan, &workerWg)
        go worker(i, dstDB, extractTableName, columns, nil, dataChan, errorChan, &workerWg)
    }

    // Process data from Oracle in partitioned mode
    var migrationError error

    if strings.ToLower(partitionBy) == "rownum" {
        var totalRows int64
        countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE 1=1 %s", srcTable, filterWhere)
        if err := srcDB.QueryRow(countQuery).Scan(&totalRows); err != nil {
            log.Fatalf("Error retrieving total rows %v from query: %v", err, countQuery)
            migrationError = fmt.Errorf("Error retrieving total rows: %v", err)
        }
        log.Printf("Total rows: %d", totalRows)

        partitionCount := numWorkers

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
                    log.Printf("call done()")
                    partitionWg.Done()
                }()
                
                if startRow > endRow {
                    log.Printf("Partition %d: No data to process (startRow: %d, endRow: %d)", partitionId, startRow, endRow)
                    return
                }

                query := fmt.Sprintf("SELECT %s FROM (SELECT t.*, rownum rnum FROM %s t WHERE 1=1 %s) WHERE rnum BETWEEN %d AND %d", colList, srcTable, filterWhere, startRow, endRow)
                t := time.Now()
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
                    case "VARCHAR2", "VARCHAR", "CHAR", "NCHAR", "NVARCHAR2", "TEXT":  //CLOB??
                        var s sql.NullString
                        scanArgs[i] = &s // use sql.NullString to handle NULL values
                    default:
                        var raw interface{}
                        scanArgs[i] = &raw
                    }
                }

                
                for rows.Next() {
                    if err := rows.Scan(scanArgs...); err != nil {
                        log.Printf("Partition %d: row scan error: %v", partitionId, err)
                        migrationError = fmt.Errorf("Partition %d: row scan error: %v", partitionId, err)
                        continue
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

                    if count%LOG_READED_ROWS == 0 {
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

    } else if partitionColumn != "" {
        // NOT TEST !!!!
        var minVal, maxVal int64
        queryMinMax := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", partitionColumn, partitionColumn, srcTable)
        row := srcDB.QueryRow(queryMinMax)
        if err := row.Scan(&minVal, &maxVal); err != nil {
            log.Fatalf("Error scanning min/max from Oracle: %v", err)
            migrationError = fmt.Errorf("Error retrieving total rows: %v", err)
        }
        log.Printf("Minimum and maximum values of %s: %d - %d", partitionColumn, minVal, maxVal)

        partitionCount := 8
        if s := os.Getenv("PARTITION_COUNT"); s != "" {
            if cnt, err := strconv.Atoi(s); err == nil && cnt > 0 {
                partitionCount = cnt
            }
        }

        rangeSize := (maxVal - minVal + 1) / int64(partitionCount)
        if rangeSize == 0 {
            rangeSize = 1
        }

        var partitionWg sync.WaitGroup
        for i := 0; i < partitionCount; i++ {
            startVal := minVal + int64(i)*rangeSize
            var endVal int64
            if i == partitionCount-1 {
                endVal = maxVal
            } else {
                endVal = startVal + rangeSize - 1
            }
            partitionWg.Add(1)
            go func(startVal, endVal int64, partitionId int) {
                defer partitionWg.Done()
                query := fmt.Sprintf("SELECT * FROM %s WHERE %s BETWEEN %d AND %d", srcTable, partitionColumn, startVal, endVal)
                rows, err := srcDB.Query(query)
                if err != nil {
                    log.Printf("Partition %d: query execution error: %v", partitionId, err)
                    migrationError = fmt.Errorf("Partition %d: query error: %v", partitionId, err)
                    
                    return
                }
                defer rows.Close()

                colCount := len(columns)
                count := 0
                scanArgs := make([]interface{}, colCount)
                for i := range scanArgs {
                    scanArgs[i] = new(interface{})
                }
                rowData := make([]interface{}, colCount)
                for rows.Next() {
                    if err := rows.Scan(scanArgs...); err != nil {
                        log.Printf("Partition %d: row scan error: %v", partitionId, err)
                        migrationError = fmt.Errorf("Partition %d: row scan error: %v", partitionId, err)
                        
                        continue
                    }
                    
                    for i, ptr := range scanArgs {
                        rowData[i] = *(ptr.(*interface{}))
                    }
                    dataChan <- rowData
                    count++
                    if count%LOG_READED_ROWS == 0 {
                        log.Printf("Partition %d: Read %d rows", partitionId, count)
                    }
                }
                if err := rows.Err(); err != nil {
                    log.Printf("Partition %d: error iterating over rows: %v", partitionId, err)
                    migrationError = fmt.Errorf("Partition %d: error iterating over rows: %v", partitionId, err)
                }
                log.Printf("Partition %d: finished reading, total rows: %d", partitionId, count)
            }(startVal, endVal, i)
        }
        partitionWg.Wait()
    } else {
        // NOT TEST !!!!
        query := fmt.Sprintf("SELECT * FROM %s WHERE 1=1 %s", srcTable, filterWhere)
        rows, err := srcDB.Query(query)
        if err != nil {
            log.Fatalf("Error executing query on Oracle: %v", err)
            migrationError = fmt.Errorf("Error retrieving total rows: %v", err)
        }
        defer rows.Close()

        colCount := len(columns)
        count := 0
        scanArgs := make([]interface{}, colCount)
        for i := range scanArgs {
            scanArgs[i] = new(interface{})
        }

        for rows.Next() {
            if err := rows.Scan(scanArgs...); err != nil {
                log.Printf("Error scanning row: %v", err)
                migrationError = fmt.Errorf("Error scanning row: %v", err)
                        
                continue
            }
            rowData := make([]interface{}, colCount)
            for i, ptr := range scanArgs {
                rowData[i] = *(ptr.(*interface{}))
            }
            dataChan <- rowData
            count++
            if count%LOG_READED_ROWS == 0 {
                log.Printf("Read %d rows from Oracle", count)
            }
        }
        if err = rows.Err(); err != nil {
            log.Fatalf("Error iterating over rows: %v", err)
            migrationError = fmt.Errorf("Error iterating over rows: %v", err)
        }
    }

    log.Printf("Closing channel for table: %s", srcTable)
    // Close the channel after reading is complete
    close(dataChan)

    // Wait for workers to finish inserting data
    workerWg.Wait()

    // Check for errors from workers
    close(errorChan)

    for err := range errorChan {
        if migrationError == nil {
            migrationError = err
        } else {
            log.Printf("Additional error: %v", err)
        }
    }

    if migrationError != nil {
        log.Printf("Error occurred while migrating data from table: %s. Error: %v", srcTable, migrationError)
        return migrationError
    }

    log.Printf("Finished migrating data from table: %s", srcTable)
    return nil
}