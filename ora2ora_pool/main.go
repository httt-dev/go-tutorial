package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/sijms/go-ora/v2"
)

var(
	expectedDBName string
)

const (
	CHAN_QUEUE = 50_000
	BATCH_SIZE = 50_000
	LOG_READED_ROWS =  100_000
)
func init() {
    err := godotenv.Load()
    if err != nil {
        log.Printf("Could not find .env file, using system environment variables: %v", err)
    }
}

// Function to set up logging
func setupLogging() {
    // Get the current time to name the log file
    now := time.Now()
    logFileName := fmt.Sprintf("log_%s.log", now.Format("20060102_150405"))

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

func printMemUsage(tag string) {
    var m runtime.MemStats
    runtime.ReadMemStats(&m)
    fmt.Printf("%s - Alloc = %v MiB, TotalAlloc = %v MiB, Sys = %v MiB, NumGC = %v\n",
        tag, m.Alloc/1024/1024, m.TotalAlloc/1024/1024, m.Sys/1024/1024, m.NumGC)
}

func insertBatch(db *sql.DB, tableName string, columns []string, batch [][]interface{}) error {
    ctx := context.Background()

    // Create the INSERT statement with placeholders
    sqlText := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
        tableName,
        strings.Join(columns, ", "),
        ":"+strings.Join(columns, ", :"),
    )

    tx, err := db.BeginTx(ctx, nil)
    if err != nil {
        return fmt.Errorf("error starting transaction: %w", err)
    }
    defer tx.Rollback() // Rollback transaction in case of error

    stmt, err := tx.PrepareContext(ctx, sqlText)
    if err != nil {
        return fmt.Errorf("error preparing statement: %w", err)
    }
    defer stmt.Close()

    // Convert batch into slices for each column
    colCount := len(columns)
    colSlices := make([][]interface{}, colCount)
    for i := range colSlices {
        colSlices[i] = make([]interface{}, len(batch))
    }
    for rowIdx, row := range batch {
        for colIdx, col := range row {
            colSlices[colIdx][rowIdx] = col
        }
    }

    // Create a slice containing named parameters
    namedArgs := make([]interface{}, colCount)
    for i, col := range columns {
        namedArgs[i] = sql.Named(col, colSlices[i])
    }
    
    // Execute the INSERT statement with slices
    _, err = stmt.ExecContext(ctx, namedArgs...)
    if err != nil {
        return fmt.Errorf("error executing batch insert: %w", err)
    }

    err = tx.Commit()
    if err != nil {
        return fmt.Errorf("error committing transaction: %w", err)
    }

    return nil
}

// Function to get column data types from a table
func getColumnTypes(db *sql.DB, tableName string) ([]interface{}, error) {
    query := `
        SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE
        FROM ALL_TAB_COLUMNS
        WHERE TABLE_NAME = :1`
    
    ctx := context.Background()
    rows, err := db.QueryContext(ctx, query, strings.ToUpper(tableName))
    if err != nil {
        return nil, fmt.Errorf("error querying column types: %w", err)
    }
    defer rows.Close()

    var columnTypes []interface{}
    for rows.Next() {
        var columnName, dataType string
        var dataPrecision, dataScale sql.NullInt64 // Can be NULL if not applicable

        if err := rows.Scan(&columnName, &dataType, &dataPrecision, &dataScale); err != nil {
            return nil, fmt.Errorf("error scanning row: %w", err)
        }

        // Handle NUMBER type
        if strings.ToUpper(dataType) == "NUMBER" {
            if dataScale.Valid && dataScale.Int64 > 0 { // Has decimal part → float64
                columnTypes = append(columnTypes, float64(0))
            } else { // Larger numbers → int64
                columnTypes = append(columnTypes, int64(0))
            }
            continue
        }

        // Handle other data types
        switch strings.ToUpper(dataType) {
        case "VARCHAR2", "CHAR", "NVARCHAR2", "CLOB":
            columnTypes = append(columnTypes, "")
        case "DATE", "TIMESTAMP", "TIMESTAMP(6)":
            columnTypes = append(columnTypes, time.Time{})
        case "BLOB", "RAW":
            columnTypes = append(columnTypes, []byte{})
        default:
            columnTypes = append(columnTypes, nil)
        }
    }

    if err := rows.Err(); err != nil {
        return nil, fmt.Errorf("error iterating over rows: %w", err)
    }

    return columnTypes, nil
}

// Escape string to prevent SQL Injection
func escapeString(value string) string {
    return strings.ReplaceAll(value, "'", "''")
}

func formatValue(value interface{}) string {
    switch v := value.(type) {
    case nil:
        return "NULL" // Oracle uses NULL to represent empty values
    case string:
        return fmt.Sprintf("'%s'", escapeString(v)) // Add single quotes and handle special characters
    case time.Time:
        if v.IsZero() {
            return "NULL"
        }
        return fmt.Sprintf("TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF6')", v.Format("2006-01-02 15:04:05.000000"))
    case int, int32, int64, float32, float64:
        return fmt.Sprintf("%v", v) // Numeric types don't need single quotes
    default:
        return fmt.Sprintf("'%v'", v) // Default to adding single quotes
    }
}

// insertBatchWithFallback retains the old logic to handle errors
func insertBatchWithFallback(db *sql.DB, tableName string, columns []string, columnTypes []interface{}, batch [][]interface{}) error {
    err := insertBatch(db, tableName, columns, batch)

    if err == nil {
        return nil
    }
    // If the batch contains only one record, log it and return the error
    if len(batch) == 1 {
        log.Printf("Error record: %v", batch[0])
        return err
    }
    // Split the batch into two parts and try inserting each part
    mid := len(batch) / 2
    err1 := insertBatchWithFallback(db, tableName, columns, columnTypes, batch[:mid])
    err2 := insertBatchWithFallback(db, tableName, columns, columnTypes, batch[mid:])
    if err1 != nil || err2 != nil {
        return fmt.Errorf("Error inserting split batch: err1: %v, err2: %v", err1, err2)
    }
    return nil
}

// Worker function to insert data into Oracle
func worker(workerId int, db *sql.DB, dstTable string, columns []string, columnTypes []interface{}, dataChan <-chan []interface{}, wg *sync.WaitGroup) {
    defer wg.Done()
    batchSize := BATCH_SIZE // Number of rows per batch (adjustable)
    batch := make([][]interface{}, 0, batchSize)

    actualDBName, err := verifyConnection(db)
    if err != nil {
        log.Fatalf("Failed to verify connection: %v", err)
    }
    if actualDBName != expectedDBName {
        log.Fatalf("Wrong database connected! Expected: %s, Actual: %s", expectedDBName, actualDBName)
    }
    log.Printf("Connected to the correct database: %s , workerId: %v", actualDBName,workerId)

    for row := range dataChan {
        batch = append(batch, row)
        if len(batch) >= batchSize {
            if err := insertBatchWithFallback(db, dstTable, columns, columnTypes, batch); err != nil {
                log.Printf("Worker %d: error inserting batch: %v", workerId, err)
            }
            batch = batch[:0]
        }
    }
    // Insert remaining data in the batch
    if len(batch) > 0 {
        if err := insertBatchWithFallback(db, dstTable, columns, columnTypes, batch); err != nil {
            log.Printf("Worker %d: error inserting last batch: %v", workerId, err)
        }
    }
    log.Printf("Worker %d: finished processing", workerId)
}

func createOraclePool(dsn string) (*sql.DB, error) {
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

func verifyConnection(db *sql.DB) (string, error) {
    ctx := context.Background()
    var dbName string
    query := "SELECT SYS_CONTEXT('USERENV', 'DB_NAME') AS DB_NAME FROM DUAL"
    err := db.QueryRowContext(ctx, query).Scan(&dbName)
    if err != nil {
        return "", fmt.Errorf("error verifying connection: %w", err)
    }
    return dbName, nil
}

func disableLogging(db *sql.DB, tableName string) error {
    actualDBName, err := verifyConnection(db)
    if err != nil {
        log.Fatalf("Failed to verify connection: %v", err)
    }
    if actualDBName != expectedDBName {
        log.Fatalf("Wrong database connected! Expected: %s, Actual: %s", expectedDBName, actualDBName)
    }
    log.Printf("Connected to the correct database: %s", actualDBName)

    ctx := context.Background()
    _, err = db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s NOLOGGING", tableName))
    if err != nil {
        return fmt.Errorf("failed to disable logging: %w", err)
    }
    log.Printf("Disabled logging for table: %s", tableName)
    return nil
}

func enableLogging(db *sql.DB, tableName string) error {
    actualDBName, err := verifyConnection(db)
    if err != nil {
        log.Fatalf("Failed to verify connection: %v", err)
    }
    if actualDBName != expectedDBName {
        log.Fatalf("Wrong database connected! Expected: %s, Actual: %s", expectedDBName, actualDBName)
    }
    log.Printf("Connected to the correct database: %s", actualDBName)
    ctx := context.Background()
    _, err = db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s LOGGING", tableName))
    if err != nil {
        return fmt.Errorf("failed to enable logging: %w", err)
    }
    log.Printf("Enabled logging for table: %s", tableName)
    return nil
}

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
        dstTable := srcTable // Mặc định bảng đích giống bảng nguồn
        if len(parts) > 1 {
            dstTable = strings.TrimSpace(parts[1])
        }
        mappings[srcTable] = dstTable
    }
    return mappings
}
func main() {
    // Set up logging
    setupLogging()

    startTime := time.Now()
    defer func() {
        log.Printf("Total execution time: %v", time.Since(startTime))
    }()

    // Retrieve required environment variables
    srcOracleDSN := os.Getenv("SRC_ORACLE_DSN")
    dstOracleDSN := os.Getenv("DST_ORACLE_DSN")
    expectedDBName = os.Getenv("DESTINATION_DB_NAME")

    if srcOracleDSN == "" || dstOracleDSN == "" || expectedDBName == "" {
        log.Fatal("Please set the environment variables SRC_ORACLE_DSN, DST_ORACLE_DSN, DESTINATION_DB_NAME")
    }

    tableNames := os.Getenv("TABLE_NAME")
    if tableNames == "" {
        log.Fatal("Please set the environment variable TABLE_NAME")
    }
    tableMappings := parseTableMappings(tableNames)
    tbl, err := json.MarshalIndent(tableMappings, "", "  ")
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

    // Connect to Destination Oracle
    dstOracleDB, err := createOraclePool(dstOracleDSN)
    if err != nil {
        log.Fatalf("Error creating DESTINATION Oracle pool: %v", err)
    }
    defer dstOracleDB.Close()

    for srcTable, dstTable := range tableMappings {
        log.Printf("Starting to process table: source=%s, destination=%s", srcTable, dstTable)

        // Disable logging before starting
        err := disableLogging(dstOracleDB, dstTable)
        if err != nil {
            log.Printf("Error disabling logging for table %s: %v", dstTable, err)
            continue
        }

        defer func(dstTable string) {
            if err := enableLogging(dstOracleDB, dstTable); err != nil {
                log.Printf("Error enabling logging for table %s: %v", dstTable, err)
            } 
        }(dstTable)

        // Perform the data migration process for the current table
        err = migrateTable(srcOracleDB, dstOracleDB, srcTable, dstTable, partitionBy, partitionColumn, filterWhere)
        if err != nil {
            log.Printf("Error processing table source=%s, destination=%s: %v", srcTable, dstTable, err)
            continue
        }
        log.Printf("Finished processing table: source=%s, destination=%s", srcTable, dstTable)
    }

    log.Printf("Data migration completed.")
}

func migrateTable(srcDB, dstDB *sql.DB, srcTable string, dstTable string, partitionBy string, partitionColumn string, filterWhere string) error {
    // Retrieve column names from Oracle
    dummyQuery := fmt.Sprintf("SELECT * FROM %s WHERE 1=0", srcTable)
    dummyRows, err := srcDB.Query(dummyQuery)
    if err != nil {
        log.Fatalf("Error executing dummy query: %v", err)
    }
    columns, err := dummyRows.Columns()
    if err != nil {
        log.Fatalf("Error getting columns: %v", err)
    }
    dummyRows.Close()

    // Create a channel to transmit rows of data
    dataChan := make(chan []interface{}, CHAN_QUEUE)

    // Initialize worker pool for Oracle
    numWorkers := 10
    if s := os.Getenv("PARTITION_COUNT"); s != "" {
        if cnt, err := strconv.Atoi(s); err == nil && cnt > 0 {
            numWorkers = cnt
        }
    }
    var workerWg sync.WaitGroup
    for i := 0; i < numWorkers; i++ {
        workerWg.Add(1)
        go worker(i, dstDB, dstTable, columns, nil, dataChan, &workerWg)
    }

    // Process data from Oracle in partitioned mode
    if strings.ToLower(partitionBy) == "rownum" {
        var totalRows int64
        countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE 1=1 %s", srcTable, filterWhere)
        if err := srcDB.QueryRow(countQuery).Scan(&totalRows); err != nil {
            log.Fatalf("Error retrieving total rows: %v", err)
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
                    return
                }
                log.Printf("Finish query with partitionId %d: time : %v", partitionId, time.Now().Sub(t))

                defer rows.Close()

                colCount := len(columns)
                count := 0
                scanArgs := make([]interface{}, colCount)
                for i := range scanArgs {
                    scanArgs[i] = new(interface{})
                }

                for rows.Next() {
                    if err := rows.Scan(scanArgs...); err != nil {
                        log.Printf("Partition %d: row scan error: %v", partitionId, err)
                        continue
                    }
                    rowData := make([]interface{}, colCount)
                    for i, ptr := range scanArgs {
                        rowData[i] = *(ptr.(*interface{}))
                    }
                    dataChan <- rowData
                    count++

                    if count%LOG_READED_ROWS == 0 {
                        log.Printf("Partition %d: Read %d rows", partitionId, count)
                    }
                }

                if err = rows.Err(); err != nil {
                    log.Printf("Partition %d: error iterating over rows: %v", partitionId, err)
                }

                log.Printf("Partition %d: finished reading, total rows: %d", partitionId, count)
            }(startRow, endRow, i)
        }

        partitionWg.Wait()

    } else if partitionColumn != "" {
        var minVal, maxVal int64
        queryMinMax := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", partitionColumn, partitionColumn, srcTable)
        row := srcDB.QueryRow(queryMinMax)
        if err := row.Scan(&minVal, &maxVal); err != nil {
            log.Fatalf("Error scanning min/max from Oracle: %v", err)
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
                }
                log.Printf("Partition %d: finished reading, total rows: %d", partitionId, count)
            }(startVal, endVal, i)
        }
        partitionWg.Wait()
    } else {
        query := fmt.Sprintf("SELECT * FROM %s WHERE 1=1 %s", srcTable, filterWhere)
        rows, err := srcDB.Query(query)
        if err != nil {
            log.Fatalf("Error executing query on Oracle: %v", err)
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
        }
    }

    log.Printf("Closing channel for table: %s", srcTable)
    // Close the channel after reading is complete
    close(dataChan)

    // Wait for workers to finish inserting data
    workerWg.Wait()
    log.Printf("Finished migrating data from table: %s", srcTable)
    return nil
}
