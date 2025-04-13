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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	_ "github.com/sijms/go-ora/v2"
)

type TableSummary struct {
    SourceTable      string
    SourceRowCount   int64
    DestinationTable string
    CopiedRowCount   int64
    Status           string
}

var summaryList []TableSummary

var(
	expectedDBName string
)

const(
	FETCHED_NUM_ROWS = 100_000
	CHAN_QUEUE = 100_000
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

// verifyConnection checks the connection to the database (assumes it already exists)
func verifyConnection(pool *pgxpool.Pool) (string, error) {
	var dbName string
	err := pool.QueryRow(context.Background(), "SELECT current_database()").Scan(&dbName)
	if err != nil {
		return "", err
	}
	return dbName, nil
}

// DisableConstraints disables all constraints of the table
func disableConstraints(pool *pgxpool.Pool, tableName string) error {
	actualDBName, err := verifyConnection(pool)
	if err != nil {
		log.Fatalf("Failed to verify connection: %v", err)
	}
	if actualDBName != expectedDBName {
		log.Fatalf("Wrong database connected! Expected: %s, Actual: %s", expectedDBName, actualDBName)
	}
	log.Printf("Connected to the correct database: %s", actualDBName)

	ctx := context.Background()
	_, err = pool.Exec(ctx, fmt.Sprintf("ALTER TABLE %s DISABLE TRIGGER ALL", tableName))
	if err != nil {
		return fmt.Errorf("failed to disable constraints: %w", err)
	}
	log.Printf("Disabled constraints for table: %s", tableName)
	return nil
}

// EnableConstraints enabled all constraints of the table
func enableConstraints(pool *pgxpool.Pool, tableName string) error {
	actualDBName, err := verifyConnection(pool)
	if err != nil {
		log.Fatalf("Failed to verify connection: %v", err)
	}
	if actualDBName != expectedDBName {
		log.Fatalf("Wrong database connected! Expected: %s, Actual: %s", expectedDBName, actualDBName)
	}
	log.Printf("Connected to the correct database: %s", actualDBName)

	ctx := context.Background()
	_, err = pool.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ENABLE TRIGGER ALL", tableName))
	if err != nil {
		return fmt.Errorf("failed to enable constraints: %w", err)
	}
	log.Printf("Enabled constraints for table: %s", tableName)
	return nil
}

// insertBatch performs batch insertion (multiple rows) into PostgreSQL using pgx.CopyFrom
func insertBatch(pool *pgxpool.Pool, tableName string, columns []string, batch [][]interface{}) error {
	defer timeTrack(time.Now(), "insertBatch full")
	ctx := context.Background()

	// start := time.Now()
	// copyData := make([][]interface{}, len(batch))
	// for i, row := range batch {
	// 	copyData[i] = row
	// }
	// fmt.Println("Prepare data took:", time.Since(start))

	// copy(copyData, batch)

	// start = time.Now()
	// stats := pool.Stat()
	// fmt.Printf("Pool stats before CopyFrom: %d/%d connections in use\n", stats.AcquiredConns(), stats.MaxConns())

	_, err := pool.CopyFrom(ctx, pgx.Identifier{tableName}, columns, pgx.CopyFromRows(batch))
	// fmt.Println("CopyFrom took:", time.Since(start))

	if err != nil {
		return fmt.Errorf("copy from error for table %s with %d rows: %w", tableName, len(batch), err)
	}

	return nil
}


func insertBatchWithFallback(pool *pgxpool.Pool, tableName string, columns []string, batch [][]interface{}) error {
    err := insertBatch(pool, tableName, columns, batch)
    if err == nil {
        return nil
    }else{
		return fmt.Errorf("error insertingbbatch: err: %w", err)
	}

    // if len(batch) == 1 {
    //     log.Printf("Error record: %v. Error detail %v", batch[0],err)
    //     return err
    // }

    // mid := len(batch) / 2
    // err1 := insertBatchWithFallback(pool, tableName, columns, batch[:mid])
    // err2 := insertBatchWithFallback(pool, tableName, columns, batch[mid:])
    // if err1 != nil || err2 != nil {
    //     return fmt.Errorf("error inserting split batch: err1: %v, err2: %v", err1, err2)
    // }
    // return nil
}

func worker(ctx context.Context, workerId int, pool *pgxpool.Pool, dstTable string, columns []string, dataChan <-chan []interface{}, errorChan chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()

	batchSize := BATCH_SIZE // Number of rows per batch (adjustable)
	batch := make([][]interface{}, 0, batchSize)

	// actualDBName, err := verifyConnection(pool)
    // if err != nil {
    //     errorChan <- fmt.Errorf("Worker %d: failed to verify connection: %v", workerId, err)
    //     return
    // }
    // if actualDBName != expectedDBName {
    //     errorChan <- fmt.Errorf("Worker %d: wrong database connected! Expected: %s, Actual: %s", workerId, expectedDBName, actualDBName)
    //     return
    // }
    // log.Printf("Worker %d: connected to the correct database: %s", workerId, actualDBName)

	// for row := range dataChan {
	// 	batch = append(batch, row)
	// 	if len(batch) >= batchSize {
	// 		if err := insertBatchWithFallback(pool, dstTable, columns, batch); err != nil {
	// 			// log.Printf("Worker %d: error inserting batch: %v", workerId, err)
	// 			errorChan <- fmt.Errorf("worker %d: error inserting batch: %v", workerId, err)
	// 		}
	// 		batch = batch[:0]
	// 	}
	// }

	// // Insert remaining data in the batch
	// if len(batch) > 0 {
	// 	if err := insertBatchWithFallback(pool, dstTable, columns, batch); err != nil {
	// 		// log.Printf("Worker %d: error inserting last batch: %v", workerId, err)
	// 		errorChan <- fmt.Errorf("worker %d: error inserting batch: %v", workerId, err)
	// 	}
	// }

	for{
		select{
			case <-ctx.Done():
				log.Printf("Worker %d: received cancellation signal, stopping...", workerId)
            	return
			case row, ok := <-dataChan:
				if !ok {
					// Channel closed, process remaining batch
					if len(batch) > 0 {
						if err := insertBatchWithFallback(pool, dstTable, columns, batch); err != nil {
							errorChan <- fmt.Errorf("worker %d: error inserting last batch: %v", workerId, err)
							return // Exit immediately if the last batch fails
						}
					}
					log.Printf("Worker %d: finished processing remaining rows", workerId)
					return
				}
				batch = append(batch, row)
				if len(batch) >= batchSize {
					if err := insertBatchWithFallback(pool, dstTable, columns, batch); err != nil {
						errorChan <- fmt.Errorf("worker %d: error inserting batch: %v", workerId, err)
					}
					batch = batch[:0]
				}
				
		}
	}
	log.Printf("Worker %d: finished processing", workerId)

}

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}

// Helper function to extract host and service from the DSN
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

// func createOraclePool(dsn string, timezone string) (*sql.DB, error) {
// 	// Phân tích DSN để lấy các thông số kết nối
// 	params, err := godror.ParseDSN(dsn)
// 	if err != nil {
// 		return nil, fmt.Errorf("lỗi phân tích DSN: %w", err)
// 	}

// 	// Chuyển đổi chuỗi timezone thành *time.Location
// 	loc, err := time.LoadLocation(timezone)
// 	if err != nil {
// 		return nil, fmt.Errorf("lỗi load timezone %s: %w", timezone, err)
// 	}
// 	params.Timezone = loc // Gán timezone đúng kiểu dữ liệu

// 	// Cấu hình connection pooling
// 	params.SessionTimeout = 60 * time.Second
// 	params.WaitTimeout = 30 * time.Second
// 	params.MaxSessions = 20
// 	params.MinSessions = 5
// 	params.SessionIncrement = 2
// 	params.Charset = "UTF-8"
	
// 	// Mở connection pool
// 	db, err := sql.Open("godror", params.StringWithPassword())
// 	if err != nil {
// 		return nil, fmt.Errorf("error connecting to Oracle: %w", err)
// 	}

// 	// Kiểm tra kết nối
// 	if err = db.Ping(); err != nil {
// 		db.Close()
// 		return nil, fmt.Errorf("error testing connection: %w", err)
// 	}
// 	log.Println("Successfully connected to Oracle.")
// 	return db, nil
// }

func createPgPool(pgDSN string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(pgDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	config.MaxConns = 50
	config.MinConns = 5
	config.MaxConnLifetime = time.Hour
	config.MaxConnIdleTime = 30 * time.Minute
	config.HealthCheckPeriod = 1 * time.Minute

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to create pool: %w", err)
	}
	if err = pool.Ping(context.Background()); err != nil {
		pool.Close()
		return nil, fmt.Errorf("error testing connection: %w", err)
	}

	log.Println("Successfully connected to PostgreSQL.")
	return pool, nil
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
        dstTable := srcTable 
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
		// Print summary
		printSummary()

        log.Printf("Total execution time: %v", time.Since(startTime))
    }()

	// Retrieve required environment variables
	oracleDSN := os.Getenv("ORACLE_DSN")
	pgDSN := os.Getenv("PG_DSN")
	expectedDBName = os.Getenv("DESTINATION_DB_NAME")

	if oracleDSN == "" || pgDSN == "" || expectedDBName == "" {
		log.Fatal("Please set the environment variables ORACLE_DSN, PG_DSN, DESTINATION_DB_NAME")
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

	// Variables to determine partitioning mode:
	// If PARTITION_BY = "rownum" -> partition by rownum
	// If PARTITION_BY is different and PARTITION_COLUMN is set -> partition by column
	partitionBy := os.Getenv("PARTITION_BY")
	partitionColumn := os.Getenv("PARTITION_COLUMN")
	filterWhere := os.Getenv("FILTER")
	if strings.Trim(filterWhere," ") =="" {
		filterWhere = " AND 1=1 "
	}else{
		filterWhere = " AND " + filterWhere
	}
	// Connect to Source Oracle
	// oracleDB, err := createOraclePool(oracleDSN,"Asia/Tokyo")
	oracleDB, err := createOraclePool(oracleDSN)
	if err != nil {
		log.Fatalf("Error creating Oracle pool: %v", err)
	}
	defer oracleDB.Close()

	// Connect to Destination Oracle
	posgresDB, err := createPgPool(pgDSN)
	if err != nil {
		log.Fatalf("Error creating PostgreSQL pool: %v", err)
	}
	defer posgresDB.Close()

	for srcTable, dstTable := range tableMappings {
		log.Printf(strings.Repeat("-", 20) + "Starting to process table: source=%s, destination=%s" + strings.Repeat("-", 20), srcTable, dstTable)
		
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// Disable logging before starting
        err := disableConstraints(posgresDB, dstTable)
        if err != nil {
            log.Printf("Error disabling constraint for table %s: %v", dstTable, err)
            continue
        }

        defer func(dstTable string) {
            if err := enableConstraints(posgresDB, dstTable); err != nil {
                log.Printf("Error enabling constraint for table %s: %v", dstTable, err)
            } 
        }(dstTable)

		// Perform the data migration process for the current table
        err = migrateTable(ctx, cancel, oracleDB, posgresDB, srcTable, dstTable, partitionBy, partitionColumn, filterWhere)
        if err != nil {
            log.Printf("Error processing table source=%s, destination=%s: %v", srcTable, dstTable, err)
            continue
        }
        log.Printf("Finished processing table: source=%s, destination=%s", srcTable, dstTable)
	}
	

    log.Printf("Data migration completed.")


}

func migrateTable(ctx context.Context, cancel context.CancelFunc, srcDB *sql.DB,dstDB *pgxpool.Pool , srcTable string, dstTable string, partitionBy string, partitionColumn string, filterWhere string) error {
	var sourceRowCount, copiedRowCount int64
	var migrationError error
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

	log.Printf("Columns of the source table: %v", columns)
	for i, col := range columns {
		columns[i] = strings.ToLower(col)
	}
	log.Printf("Columns (after converting to lowercase): %v", columns)

 	// Initialize worker pool for Oracle
	numWorkers := 10
	if s := os.Getenv("PARTITION_COUNT"); s != "" {
		if cnt, err := strconv.Atoi(s); err == nil && cnt > 0 {
			numWorkers = cnt
		}
	}
	
	partitionCount := numWorkers

	// Create a channel to transmit rows of data
	dataChan := make(chan []interface{}, CHAN_QUEUE)

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

	
	var workerWg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		workerWg.Add(1)
		// go worker(i, pool, tableName, columns, dataChan, &workerWg)
		go worker(ctx, i, dstDB, dstTable, columns, dataChan, errorChan, &workerWg)
	}

	// Process data from Oracle in partitioned mode
	if strings.ToLower(partitionBy) == "rownum" {
		// parition by rownum
		var totalRows int64
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE 1=1 %s", srcTable , filterWhere)
		if err := srcDB.QueryRow(countQuery).Scan(&totalRows); err != nil {
			log.Fatalf("Error retrieving total rows: %v", err)
			migrationError = fmt.Errorf("error retrieving total rows: %v", err)
		}
		log.Printf("Total rows: %d", totalRows)
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
                    log.Printf("call done()")
                    partitionWg.Done()
                }()
                
                if startRow > endRow {
                    log.Printf("Partition %d: No data to process (startRow: %d, endRow: %d)", partitionId, startRow, endRow)
                    return
                }

				query := fmt.Sprintf("SELECT %s FROM (SELECT t.*, rownum rnum FROM %s t WHERE 1=1 %s) WHERE rnum BETWEEN %d AND %d", colList, srcTable, filterWhere, startRow, endRow)
				t := time.Now()

				select {
				case <-ctx.Done():
					log.Printf("Partition %d: Received cancellation signal, stopping...", partitionId)
					return
				default:
					// Tiếp tục nếu không có tín hiệu hủy
				}

				rows, err := srcDB.Query(query)
				if err != nil {
					log.Printf("Partition %d: query error: %v", partitionId, err)
					migrationError = fmt.Errorf("partition %d: query error: %v", partitionId, err)
                    return
				}
				log.Printf("Finish query with partitionId %d: time : %v", partitionId, time.Since(t))
				defer rows.Close()

				colCount := len(columns)
				count := 0

				scanArgs := make([]interface{}, colCount)
				for i := range scanArgs {
					scanArgs[i] = new(interface{})
				}

				for rows.Next() {

					select {
					case <-ctx.Done():
						log.Printf("Partition %d: Received cancellation signal while processing rows, stopping...", partitionId)
						return
					default:
						// Tiếp tục nếu không có tín hiệu hủy
					}

					
					if err := rows.Scan(scanArgs...); err != nil {
						log.Printf("Partition %d: row scan error: %v", partitionId, err)
						migrationError = fmt.Errorf("partition %d: row scan error: %v", partitionId, err)
						continue
					}

					// Tạo copy của dữ liệu vì scanArgs sẽ được tái sử dụng
					rowData := make([]interface{}, colCount)
					for i, ptr := range scanArgs {
						rowData[i] = *(ptr.(*interface{}))
					}

					dataChan <- rowData
					count++
					if count%100000 == 0 {
						log.Printf("Partition %d: Read %d rows", partitionId, count)
					}
				}
				if err := rows.Err(); err != nil {
					log.Printf("Partition %d: error iterating over rows: %v", partitionId, err)
					migrationError = fmt.Errorf("partition %d: error iterating over rows: %v", partitionId, err)
				}
				log.Printf("Partition %d: finished reading, total rows: %d", partitionId, count)
			}(startRow, endRow, i)
		}
		partitionWg.Wait()

	} else if partitionColumn != "" {
		// parition by column
		var minVal, maxVal int64
		queryMinMax := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", partitionColumn, partitionColumn, srcTable)
		row := srcDB.QueryRow(queryMinMax)
		if err := row.Scan(&minVal, &maxVal); err != nil {
			log.Fatalf("Error scanning min/max from Oracle: %v", err)
            migrationError = fmt.Errorf("Error retrieving total rows: %v", err)
		}
		log.Printf("Minimum and maximum values of %s: %d - %d", partitionColumn, minVal, maxVal)

		partitionCount = numWorkers
		
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
				//rows, err := oracleDB.Query(query)
				rows, err := srcDB.Query(query)
				if err != nil {
					log.Printf("Partition %d: query execution error: %v", partitionId, err)
                    migrationError = fmt.Errorf("partition %d: query error: %v", partitionId, err)
					return
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
						log.Printf("Partition %d: row scan error: %v", partitionId, err)
                        migrationError = fmt.Errorf("partition %d: row scan error: %v", partitionId, err)
						continue
					}

					rowData := make([]interface{}, colCount)
					for i, ptr := range scanArgs {
						rowData[i] = *(ptr.(*interface{}))
					}

					dataChan <- rowData
					count++
					if count%100000 == 0 {
						log.Printf("Partition %d: Read %d rows", partitionId, count)
					}
				}
				if err := rows.Err(); err != nil {
					log.Printf("Partition %d: error iterating over rows: %v", partitionId, err)
                    migrationError = fmt.Errorf("partition %d: error iterating over rows: %v", partitionId, err)
				}
				log.Printf("Partition %d: finished reading, total rows: %d", partitionId, count)
			}(startVal, endVal, i)
		}
		partitionWg.Wait()
	} else {
		// NOT TEST !!!!
		// Nếu không có chế độ phân vùng, đọc dữ liệu bằng một query duy nhất
		query := fmt.Sprintf("SELECT * FROM %s WHERE 1=1 %s", srcTable, filterWhere)
		// rows, err := oracleDB.Query(query)
		rows, err := srcDB.Query(query)
		if err != nil {
			migrationError = fmt.Errorf("error retrieving total rows: %v", err)
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
                migrationError = fmt.Errorf("error scanning row: %v", err)
				continue
			}
			rowData := make([]interface{}, colCount)
			for i, ptr := range scanArgs {
				rowData[i] = *(ptr.(*interface{}))
			}
			dataChan <- rowData
			count++
			if count%100000 == 0 {
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

    if migrationError != nil {
        //log.Printf("Error occurred while migrating data from table: %s. Error: %v", srcTable, migrationError)
		cancel() // Cancel all workers
    }else{
		log.Printf("Successfully migrated data from table: %s", srcTable)
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE 1=1 %s", dstTable , filterWhere)
        if err := dstDB.QueryRow(context.Background(), countQuery).Scan(&copiedRowCount); err != nil {
            log.Printf("Error counting rows in destination table %s: %v", dstTable, err)
            migrationError = fmt.Errorf("error counting rows in destination table: %v", err)
        }
	}
	
	// Add summary to the list
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

	log.Printf("Finished migrating data from table: %s", srcTable)
    return migrationError
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