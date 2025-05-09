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
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

// Struct to hold table mapping information
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

var (
	expectedDBName string
	bulkInsertMode string
)

const (
	CHAN_QUEUE      = 100_000
	BATCH_SIZE      = 100_000
	LOG_READED_ROWS = 100_000

	CHAN_QUEUE_HAS_TEXT      = 100_000
	BATCH_SIZE_HAS_TEXT      = 100_000
	LOG_READED_HAS_TEXT_ROWS = 100_000
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Printf("Could not find .env file, using system environment variables: %v", err)
	}
}

func setupLogging() {
	logDir := "logs"
	if _, err := os.Stat(logDir); os.IsNotExist(err) {
		err := os.Mkdir(logDir, 0755)
		if err != nil {
			log.Fatalf("Unable to create logs directory: %v", err)
		}
	}

	now := time.Now()
	logFileName := fmt.Sprintf("%s/log_%s.log", logDir, now.Format("20060102_150405"))

	logFile, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Unable to open log file: %v", err)
	}

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

	// Create placeholders for values
	placeholders := make([]string, len(batch))
	values := make([]interface{}, 0, len(batch)*len(columns))

	for i, row := range batch {
		placeholders[i] = "(" + strings.Repeat("?,", len(columns)-1) + "?)"
		values = append(values, row...)
	}

	sqlText := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s",
		tableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("error starting transaction: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, sqlText, values...)
	if err != nil {
		return fmt.Errorf("error executing batch insert: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}

	return nil
}

func createMySQLPool(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("error connecting to MySQL: %w", err)
	}

	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(60 * time.Second)

	if err = db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("error testing MySQL connection: %w", err)
	}

	log.Println("Successfully connected to MySQL.")
	return db, nil
}

func verifyConnection(db *sql.DB) (string, error) {
	ctx := context.Background()
	var dbName string
	query := "SELECT DATABASE()"
	err := db.QueryRowContext(ctx, query).Scan(&dbName)
	if err != nil {
		return "", fmt.Errorf("error verifying MySQL connection: %w", err)
	}
	return dbName, nil
}

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
		dstTable := srcTable
		if len(parts) > 1 {
			dstTable = strings.TrimSpace(parts[1])
		}
		mappings = append(mappings, TableMapping{SrcTable: srcTable, DstTable: dstTable})
	}
	return mappings
}

func extractTableName(table string) string {
	parts := strings.Split(table, " ")
	if len(parts) > 0 {
		return strings.TrimSpace(parts[0])
	}
	return ""
}

func printSummary() {
	log.Println("=== SUMMARY ===")
	log.Printf("%-20s %-15s %-20s %-15s %-10s\n", "Source Table", "Source Rows", "Destination Table", "Copied Rows", "Status")
	log.Println(strings.Repeat("-", 80))
	for _, summary := range summaryList {
		log.Printf("%-20s %-15d %-20s %-15d %-10s\n",
			summary.SourceTable, summary.SourceRowCount, summary.DestinationTable, summary.CopiedRowCount, summary.Status)
	}
	log.Println(strings.Repeat("-", 80))
}

func getTableColumns(db *sql.DB, tableName string) ([]ColumnInfo, error) {
	query := `
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = DATABASE() AND table_name = ? 
        ORDER BY ordinal_position
    `
	rows, err := db.Query(query, tableName)
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

func hasTextColumn(db *sql.DB, tableName string) (bool, error) {
	query := `
        SELECT COUNT(*) 
        FROM information_schema.columns 
        WHERE table_schema = DATABASE() 
        AND table_name = ? 
        AND data_type IN ('text', 'longtext', 'mediumtext', 'tinytext')
    `
	var count int
	err := db.QueryRow(query, tableName).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("error checking text column: %w", err)
	}
	return count > 0, nil
}

func calculateMaxBatchSize(columnCount int) int {
	maxPlaceholders := 65535 // MySQL default
	safetyMargin := 0.9
	return int(float64(maxPlaceholders) / float64(columnCount) * safetyMargin)
}

func worker(ctx context.Context, workerId int, batchSize int, db *sql.DB, dstTable string, columns []string, dataChan <-chan []interface{}, errorChan chan<- error, wg *sync.WaitGroup, copiedRowCount *int64, hasText bool) {
	defer wg.Done()

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
				if len(batch) > 0 {
					if err := insertBatch(db, dstTable, columns, batch); err != nil {
						errorChan <- fmt.Errorf("worker %d: error inserting last batch: %v", workerId, err)
						return
					}
					atomic.AddInt64(copiedRowCount, int64(len(batch)))
				}
				log.Printf("Worker %d: finished processing remaining rows", workerId)
				return
			}
			batch = append(batch, row)
			if len(batch) >= batchSize {
				if err := insertBatch(db, dstTable, columns, batch); err != nil {
					errorChan <- fmt.Errorf("worker %d: error inserting batch: %v", workerId, err)
				} else {
					atomic.AddInt64(copiedRowCount, int64(len(batch)))
				}
				batch = batch[:0]
			}
		}
	}
}

func migrateTable(ctx context.Context, cancel context.CancelFunc, srcDB, dstDB *sql.DB, srcTable string, dstTable string, partitionBy string, partitionColumn string, filterWhere string) error {
	var sourceRowCount, copiedRowCount int64
	var migrationError error
	chanQueue := CHAN_QUEUE
	batchSize := BATCH_SIZE
	logReadedRows := LOG_READED_ROWS

	hasText, err := hasTextColumn(srcDB, srcTable)
	if err != nil {
		log.Fatalf("Error checking text column: %v", err)
	}

	if hasText {
		chanQueue = CHAN_QUEUE_HAS_TEXT
		batchSize = BATCH_SIZE_HAS_TEXT
		logReadedRows = LOG_READED_HAS_TEXT_ROWS
		log.Println("TEXT column detected, adjusting batch size and channel queue")
	}

	dummyQuery := fmt.Sprintf("SELECT * FROM `%s` WHERE 1=0", srcTable)
	dummyRows, err := srcDB.Query(dummyQuery)
	if err != nil {
		log.Fatalf("Error executing dummy query: %v", err)
	}
	columns, err := dummyRows.Columns()
	if err != nil {
		log.Fatalf("Error getting columns: %v", err)
	}
	dummyRows.Close()

	maxBatchSize := calculateMaxBatchSize(len(columns))
	if batchSize > maxBatchSize {
		chanQueue = maxBatchSize * 2
		batchSize = maxBatchSize
	}

	numWorkers := 10
	if s := os.Getenv("PARTITION_COUNT"); s != "" {
		if cnt, err := strconv.Atoi(s); err == nil && cnt > 0 {
			numWorkers = cnt
		}
	}

	partitionCount := numWorkers
	dataChan := make(chan []interface{}, chanQueue)
	errorChan := make(chan error, numWorkers)

	go func() {
		for err := range errorChan {
			if migrationError == nil {
				migrationError = err
			} else {
				log.Printf("Additional error: %v", err)
			}
			cancel()
		}
	}()

	extractTableName := extractTableName(dstTable)

	var workerWg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		workerWg.Add(1)
		go worker(ctx, i, batchSize, dstDB, extractTableName, columns, dataChan, errorChan, &workerWg, &copiedRowCount, hasText)
	}

	if partitionBy != "" {
		var totalRows int64
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM `%s` WHERE 1=1 %s", srcTable, filterWhere)
		if err := srcDB.QueryRow(countQuery).Scan(&totalRows); err != nil {
			log.Fatalf("Error retrieving total rows %v from query: %v", err, countQuery)
			migrationError = fmt.Errorf("Error retrieving total rows: %v", err)
		}
		log.Printf("Total rows: %d", totalRows)

		sourceRowCount = totalRows
		partitionCount = numWorkers

		rangeSize := totalRows / int64(partitionCount)
		if totalRows%int64(partitionCount) != 0 {
			rangeSize++
		}

		colList := "`" + strings.Join(columns, "`, `") + "`"

		var partitionWg sync.WaitGroup
		for i := 0; i < partitionCount; i++ {
			offset := int64(i) * rangeSize
			partitionWg.Add(1)
			go func(offset, rangeSize int64, partitionId int) {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("Partition %d: Panic occurred! Error: %v", partitionId, r)
					}
					partitionWg.Done()
				}()

				if rangeSize <= 0 {
					log.Printf("Partition %d: No data to process (rangeSize: %d)", partitionId, rangeSize)
					return
				}

				query := fmt.Sprintf("SELECT %s FROM `%s` WHERE 1=1 %s LIMIT %d OFFSET %d",
					colList, srcTable, filterWhere, rangeSize, offset)
				t := time.Now()

				select {
				case <-ctx.Done():
					log.Printf("Partition %d: Received cancellation signal, stopping...", partitionId)
					return
				default:
				}

				rows, err := srcDB.Query(query)
				if err != nil {
					log.Printf("Partition %d: query error: %v", partitionId, err)
					migrationError = fmt.Errorf("Partition %d: query error: %v", partitionId, err)
					return
				}
				log.Printf("Finish query with partitionId %d: time : %v", partitionId, time.Since(t))

				defer rows.Close()

				colCount := len(columns)
				colTypes, _ := rows.ColumnTypes()

				count := 0
				scanArgs := make([]interface{}, colCount)
				for i := 0; i < colCount; i++ {
					dbType := strings.ToUpper(colTypes[i].DatabaseTypeName())
					switch dbType {
					case "VARCHAR", "CHAR", "TEXT", "LONGTEXT", "MEDIUMTEXT", "TINYTEXT":
						var s sql.NullString
						scanArgs[i] = &s
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
					}

					if err := rows.Scan(scanArgs...); err != nil {
						log.Printf("Partition %d: row scan error: %v", partitionId, err)
						migrationError = fmt.Errorf("Partition %d: row scan error: %v", partitionId, err)
						continue
					}
					rowData := make([]interface{}, colCount)
					for i, v := range scanArgs {
						switch val := v.(type) {
						case *sql.NullString:
							if val.Valid {
								rowData[i] = val.String
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
			}(offset, rangeSize, i)
		}

		partitionWg.Wait()
	} else {
		colList := "`" + strings.Join(columns, "`, `") + "`"
		query := fmt.Sprintf("SELECT %s FROM `%s` WHERE 1=1 %s", colList, srcTable, filterWhere)
		rows, err := srcDB.Query(query)
		if err != nil {
			log.Fatalf("Error executing query on MySQL: %v", err)
			migrationError = fmt.Errorf("Error retrieving rows: %v", err)
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
			if count%logReadedRows == 0 {
				log.Printf("Read %d rows from MySQL", count)
			}
		}
		if err = rows.Err(); err != nil {
			log.Fatalf("Error iterating over rows: %v", err)
			migrationError = fmt.Errorf("Error iterating over rows: %v", err)
		}
	}

	log.Printf("Closing channel for table: %s", srcTable)
	close(dataChan)
	workerWg.Wait()
	close(errorChan)

	if migrationError != nil {
		cancel()
	} else {
		log.Printf("Successfully migrated data from table: %s", srcTable)
	}

	status := "Success"
	if migrationError != nil {
		status = "Failed"
	}
	summaryList = append(summaryList, TableSummary{
		SourceTable:      srcTable,
		SourceRowCount:   sourceRowCount,
		DestinationTable: dstTable,
		CopiedRowCount:   copiedRowCount,
		Status:           status,
	})

	return migrationError
}

func main() {
	setupLogging()

	startTime := time.Now()
	defer func() {
		printSummary()
		log.Printf("Total execution time: %v", time.Since(startTime))
	}()

	srcMySQLDSN := os.Getenv("SRC_MYSQL_DSN")
	dstMySQLDSN := os.Getenv("DST_MYSQL_DSN")
	expectedDBName = os.Getenv("DESTINATION_DB_NAME")
	bulkInsertMode = os.Getenv("BULK_INSERT_MODE")

	if bulkInsertMode == "" {
		bulkInsertMode = "0"
	}

	if srcMySQLDSN == "" || dstMySQLDSN == "" || expectedDBName == "" {
		log.Fatal("Please set the environment variables SRC_MYSQL_DSN, DST_MYSQL_DSN, DESTINATION_DB_NAME")
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

	srcDB, err := createMySQLPool(srcMySQLDSN)
	if err != nil {
		log.Fatalf("Error creating source MySQL pool: %v", err)
	}
	defer srcDB.Close()

	dstDB, err := createMySQLPool(dstMySQLDSN)
	if err != nil {
		log.Fatalf("Error creating destination MySQL pool: %v", err)
	}
	defer dstDB.Close()

	for _, mapping := range tableMappings {
		srcTable := mapping.SrcTable
		dstTable := mapping.DstTable

		log.Printf(strings.Repeat("-", 20)+"Starting to process table: source=%s, destination=%s"+strings.Repeat("-", 20), srcTable, dstTable)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err = migrateTable(ctx, cancel, srcDB, dstDB, srcTable, dstTable, partitionBy, partitionColumn, filterWhere)
		if err != nil {
			log.Printf("\033[31m[ERROR] Error processing table source=%s, destination=%s: %v\033[0m", srcTable, dstTable, err)
			continue
		}
		log.Printf("Finished processing table: source=%s, destination=%s", srcTable, dstTable)
	}

	log.Printf("Data migration completed.")
}
