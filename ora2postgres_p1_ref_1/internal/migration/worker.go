package migration

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"ora2postgres_p1/internal/database"
	"ora2postgres_p1/internal/models"
	"ora2postgres_p1/internal/utils"
)

// Worker handles the migration of data from Oracle to PostgreSQL
type Worker struct {
	workerID     int
	batchSize    int
	pgDB         *database.PostgresDB
	dstTable     string
	columns      []string
	columnTypes  []interface{}
	dataChan     <-chan []interface{}
	errorChan    chan<- error
	wg           *sync.WaitGroup
	copiedCount  *int64
	expectedDB   string
}

// NewWorker creates a new worker
func NewWorker(
	workerID int,
	batchSize int,
	pgDB *database.PostgresDB,
	dstTable string,
	columns []string,
	columnTypes []interface{},
	dataChan <-chan []interface{},
	errorChan chan<- error,
	wg *sync.WaitGroup,
	copiedCount *int64,
	expectedDB string,
) *Worker {
	return &Worker{
		workerID:     workerID,
		batchSize:    batchSize,
		pgDB:         pgDB,
		dstTable:     dstTable,
		columns:      columns,
		columnTypes:  columnTypes,
		dataChan:     dataChan,
		errorChan:    errorChan,
		wg:           wg,
		copiedCount:  copiedCount,
		expectedDB:   expectedDB,
	}
}

// Start starts the worker
func (w *Worker) Start(ctx context.Context) {
	defer w.wg.Done()

	actualDBName, err := w.pgDB.VerifyConnection()
	if err != nil {
		w.errorChan <- fmt.Errorf("Worker %d: failed to verify connection: %v", w.workerID, err)
		return
	}
	if actualDBName != w.expectedDB {
		w.errorChan <- fmt.Errorf("Worker %d: wrong database connected! Expected: %s, Actual: %s", w.workerID, w.expectedDB, actualDBName)
		return
	}
	log.Printf("Worker %d: connected to the correct database: %s", w.workerID, actualDBName)

	batch := make([][]interface{}, 0, w.batchSize)

	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %d: received cancellation signal, stopping...", w.workerID)
			return
		case row, ok := <-w.dataChan:
			if !ok {
				// Channel closed, process remaining batch
				if len(batch) > 0 {
					if err := w.insertBatchWithFallback(batch); err != nil {
						w.errorChan <- fmt.Errorf("worker %d: error inserting last batch: %v", w.workerID, err)
						return
					}
					atomic.AddInt64(w.copiedCount, int64(len(batch)))
				}
				log.Printf("Worker %d: finished processing remaining rows", w.workerID)
				return
			}
			batch = append(batch, row)
			if len(batch) >= w.batchSize {
				if err := w.insertBatchWithFallback(batch); err != nil {
					w.errorChan <- fmt.Errorf("worker %d: error inserting batch: %v", w.workerID, err)
				} else {
					atomic.AddInt64(w.copiedCount, int64(len(batch)))
					log.Printf("Worker %d: finished processing batch, copied %d rows", w.workerID, len(batch))
				}
				batch = batch[:0]
			}
		}
	}
}

// insertBatchWithFallback tries to insert a batch with fallback options
func (w *Worker) insertBatchWithFallback(batch [][]interface{}) error {
	err := w.pgDB.InsertBatch(w.dstTable, w.columns, batch)
	if err == nil {
		return nil
	}
	return err
}

// MigrateTable migrates data from Oracle to PostgreSQL
func MigrateTable(
	ctx context.Context,
	cancel context.CancelFunc,
	oracleDB *database.OracleDB,
	pgDB *database.PostgresDB,
	srcTable string,
	dstTable string,
	config *models.MigrationConfig,
	summaryList *[]models.TableSummary,
) error {
	var sourceRowCount, copiedRowCount int64
	var migrationError error
	var migrationErrorMutex sync.Mutex

	// Check for CLOB columns
	hasClob, err := oracleDB.HasClobColumn(srcTable)
	if err != nil {
		return fmt.Errorf("error checking CLOB column: %w", err)
	}

	if hasClob {
		config.ChanQueue = models.ChanQueueHasClob
		config.BatchSize = models.BatchSizeHasClob
		config.LogReadedRows = models.LogReadedHasClobRows
		log.Println("CLOB column detected, adjusting batch size and channel queue")
	}

	// Get column information
	columnsInfo, err := oracleDB.GetTableColumns(srcTable)
	if err != nil {
		return fmt.Errorf("error getting columns for %s: %w", srcTable, err)
	}

	columns := make([]string, len(columnsInfo))
	for i, col := range columnsInfo {
		columns[i] = strings.ToLower(col.Name)
	}

	// Initialize worker pool
	dataChan := make(chan []interface{}, config.ChanQueue)
	errorChan := make(chan error, config.PartitionCount)
	doneChan := make(chan struct{})

	// Goroutine to handle errors
	go func() {
		for err := range errorChan {
			migrationErrorMutex.Lock()
			if migrationError == nil {
				migrationError = err
				log.Printf("\033[31m[ERROR] Error during migration: %v\033[0m", err)
				cancel()
			} else {
				log.Printf("\033[31m[ERROR] Additional error: %v\033[0m", err)
			}
			migrationErrorMutex.Unlock()
		}
		close(doneChan)
	}()

	var workerWg sync.WaitGroup
	for i := 0; i < config.PartitionCount; i++ {
		workerWg.Add(1)
		worker := NewWorker(
			i,
			config.BatchSize,
			pgDB,
			dstTable,
			columns,
			nil,
			dataChan,
			errorChan,
			&workerWg,
			&copiedRowCount,
			config.ExpectedDBName,
		)
		go worker.Start(ctx)
	}

	// Process data from Oracle
	if strings.ToLower(config.PartitionBy) == "rownum" {
		totalRows, err := oracleDB.GetRowCount(srcTable, config.FilterWhere)
		if err != nil {
			return fmt.Errorf("error getting row count: %w", err)
		}
		sourceRowCount = totalRows

		rangeSize := totalRows / int64(config.PartitionCount)
		if totalRows%int64(config.PartitionCount) != 0 {
			rangeSize++
		}

		colList := strings.Join(columns, ", ")
		var partitionWg sync.WaitGroup
		for i := 0; i < config.PartitionCount; i++ {
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
						migrationErrorMutex.Lock()
						if migrationError == nil {
							migrationError = fmt.Errorf("Partition %d: Panic occurred! Error: %v", partitionId, r)
							cancel()
						}
						migrationErrorMutex.Unlock()
					}
					log.Printf("Partition %d: done", partitionId)
					partitionWg.Done()
				}()

				if startRow > endRow {
					log.Printf("Partition %d: No data to process (startRow: %d, endRow: %d)", partitionId, startRow, endRow)
					return
				}

				query := fmt.Sprintf("SELECT %s FROM (SELECT t.*, rownum rnum FROM %s t WHERE 1=1 %s) WHERE rnum BETWEEN %d AND %d", colList, srcTable, config.FilterWhere, startRow, endRow)
				if config.PartitionCount <= 1 {
					query = fmt.Sprintf("SELECT %s FROM %s WHERE 1=1 %s", colList, srcTable, config.FilterWhere)
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

				rows, err := oracleDB.Query(query)
				if err != nil {
					log.Printf("Partition %d: query error: %v", partitionId, err)
					migrationErrorMutex.Lock()
					if migrationError == nil {
						migrationError = fmt.Errorf("Partition %d: query error: %v", partitionId, err)
						cancel()
					}
					migrationErrorMutex.Unlock()
					return
				}
				log.Printf("Finish query with partitionId %d: time : %v", partitionId, time.Now().Sub(t))

				defer rows.Close()

				colCount := len(columns)
				colTypes, _ := rows.ColumnTypes()

				count := 0
				scanArgs := make([]interface{}, colCount)

				for i := 0; i < colCount; i++ {
					dbType := colTypes[i].DatabaseTypeName()
					switch dbType {
					case "VARCHAR2", "VARCHAR", "CHAR", "NCHAR", "NVARCHAR2", "TEXT", "CLOB":
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
						log.Printf("Partition %d: Received cancellation signal, stopping...", partitionId)
						return
					default:
						// continue if no cancellation signal
					}

					if err := rows.Scan(scanArgs...); err != nil {
						log.Printf("Partition %d: error scanning row: %v", partitionId, err)
						migrationErrorMutex.Lock()
						if migrationError == nil {
							migrationError = fmt.Errorf("Partition %d: error scanning row: %v", partitionId, err)
							cancel()
						}
						migrationErrorMutex.Unlock()
						return
					}

					rowData := make([]interface{}, colCount)
					for i, v := range scanArgs {
						switch val := v.(type) {
						case *sql.NullString:
							if val.Valid {
								rowData[i] = utils.MaybeDecodeShiftJIS(val.String)
							} else {
								rowData[i] = nil
							}
						default:
							rowData[i] = *(v.(*interface{}))
						}
					}

					select {
					case <-ctx.Done():
						log.Printf("Partition %d: Received cancellation signal, stopping...", partitionId)
						return
					case dataChan <- rowData:
						count++
						if count%config.LogReadedRows == 0 {
							log.Printf("Partition %d: read %d rows", partitionId, count)
						}
					}
				}

				if err := rows.Err(); err != nil {
					log.Printf("Partition %d: error iterating rows: %v", partitionId, err)
					migrationErrorMutex.Lock()
					if migrationError == nil {
						migrationError = fmt.Errorf("Partition %d: error iterating rows: %v", partitionId, err)
						cancel()
					}
					migrationErrorMutex.Unlock()
					return
				}

				log.Printf("Partition %d: finished reading, total rows: %d", partitionId, count)
			}(startRow, endRow, i)
		}

		// Wait for all partitions to finish reading
		partitionWg.Wait()
		log.Printf("Closing channel for table: %s", srcTable)
		close(dataChan)

		// Wait for all workers to finish
		workerWg.Wait()

		// Close error channel and wait for error processing to complete
		close(errorChan)
		<-doneChan

		// Check for migration errors
		migrationErrorMutex.Lock()
		if migrationError != nil {
			migrationErrorMutex.Unlock()
			*summaryList = append(*summaryList, models.TableSummary{
				SourceTable:      srcTable,
				DestinationTable: dstTable,
				SourceRowCount:   sourceRowCount,
				CopiedRowCount:   copiedRowCount,
				Status:          "\033[31m✗ Failed\033[0m",
			})
			return migrationError
		}
		migrationErrorMutex.Unlock()

		// Update summary with success
		*summaryList = append(*summaryList, models.TableSummary{
			SourceTable:      srcTable,
			DestinationTable: dstTable,
			SourceRowCount:   sourceRowCount,
			CopiedRowCount:   copiedRowCount,
			Status:          "\033[32m✔ Success\033[0m",
		})

		log.Printf("Successfully migrated data from table: %s", srcTable)
		return nil
	}

	return fmt.Errorf("unsupported partition method: %s", config.PartitionBy)
} 