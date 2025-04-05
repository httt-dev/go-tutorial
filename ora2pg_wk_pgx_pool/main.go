package main

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/godror/godror"
	_ "github.com/godror/godror" // Driver Oracle
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

const(
	FETCHED_NUM_ROWS = 100_000
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Printf("Không tìm thấy file .env, sử dụng biến môi trường hệ thống: %v", err)
	}
}

// insertBatch thực hiện chèn một batch (nhiều dòng) vào PostgreSQL sử dụng pgx.CopyFrom
func insertBatch(pool *pgxpool.Pool, tableName string, columns []string, batch [][]interface{}) error {
	ctx := context.Background()

	// Tạo slice chứa dữ liệu cho pgx.CopyFrom
	copyData := make([][]interface{}, len(batch))
	// for i, row := range batch {
	// 	copyData[i] = row
	// }
	copy(copyData, batch)

	// Sử dụng pgx.CopyFrom để chèn dữ liệu
	_, err := pool.CopyFrom(
		ctx,
		pgx.Identifier{tableName},
		columns,
		pgx.CopyFromRows(copyData),
	)

	if err != nil {
		return fmt.Errorf("copy from error: %w", err)
	}

	return nil
}

func insertBatchCopyFromStd(pool *pgxpool.Pool, tableName string, columns []string, batch [][]interface{}) error {
	ctx := context.Background()

	// Lệnh COPY
	copyCmd := fmt.Sprintf("COPY %s (%s) FROM STDIN", tableName, strings.Join(columns, ","))

	// Pipe để truyền dữ liệu vào PostgreSQL
	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		writer := bufio.NewWriter(pw)

		for _, row := range batch {
			var values []string
			for _, col := range row {
				values = append(values, formatValue(col))
			}
			fmt.Fprintln(writer, strings.Join(values, "\t"))
		}

		writer.Flush()
	}()

	// Gửi dữ liệu vào PostgreSQL qua COPY FROM STDIN
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	_, err = conn.Conn().PgConn().CopyFrom(ctx, pr, copyCmd)
	if err != nil {
		return fmt.Errorf("copy from stdin error: %w", err)
	}

	return nil
}


func formatValue(value interface{}) string {
	switch v := value.(type) {
	case nil:
		return "\\N" // PostgreSQL sử dụng "\N" để biểu diễn NULL
	case string:
		return strings.ReplaceAll(v, "\t", " ") // Tránh lỗi với ký tự tab
	case time.Time:
		return v.Format(time.RFC3339) // Chuyển về định dạng ISO8601
	default:
		return fmt.Sprintf("%v", v)
	}
}


func insertBatchWithFallback(pool *pgxpool.Pool, tableName string, columns []string, batch [][]interface{}) error {
    err := insertBatch(pool, tableName, columns, batch)
    if err == nil {
        return nil
    }

    // Nếu batch chỉ có 1 record, ghi log và trả về lỗi
    if len(batch) == 1 {
        log.Printf("Record lỗi: %v", batch[0])
        return err
    }

    // Chia nhỏ batch thành 2 phần và thử insert từng phần
    mid := len(batch) / 2
    err1 := insertBatchWithFallback(pool, tableName, columns, batch[:mid])
    err2 := insertBatchWithFallback(pool, tableName, columns, batch[mid:])
    if err1 != nil || err2 != nil {
        return fmt.Errorf("Lỗi khi insert batch chia nhỏ: err1: %v, err2: %v", err1, err2)
    }
    return nil
}

// worker dùng để chèn dữ liệu vào PostgreSQL theo batch
func worker(workerId int, pool *pgxpool.Pool, tableName string, columns []string, dataChan <-chan []interface{}, wg *sync.WaitGroup) {
	defer wg.Done()

	batchSize := 200000 // Số dòng mỗi batch (có thể điều chỉnh)
	batch := make([][]interface{}, 0, batchSize)

	for row := range dataChan {
		batch = append(batch, row)
		if len(batch) >= batchSize {
			if err := insertBatchWithFallback(pool, tableName, columns, batch); err != nil {
				log.Printf("Worker %d: error inserting batch: %v", workerId, err)
			}
			batch = batch[:0]
		}
	}

	// Nếu còn dư dữ liệu trong batch, chèn chúng
	if len(batch) > 0 {
		if err := insertBatchWithFallback(pool, tableName, columns, batch); err != nil {
			log.Printf("Worker %d: error inserting last batch: %v", workerId, err)
		}
	}

	log.Printf("Worker %d: finished processing", workerId)
}

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}

func createOraclePool(dsn string, timezone string) (*sql.DB, error) {
	// Phân tích DSN để lấy các thông số kết nối
	params, err := godror.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("lỗi phân tích DSN: %w", err)
	}

	// Chuyển đổi chuỗi timezone thành *time.Location
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		return nil, fmt.Errorf("lỗi load timezone %s: %w", timezone, err)
	}
	params.Timezone = loc // Gán timezone đúng kiểu dữ liệu

	// Cấu hình connection pooling
	params.SessionTimeout = 60 * time.Second
	params.WaitTimeout = 30 * time.Second
	params.MaxSessions = 20
	params.MinSessions = 5
	params.SessionIncrement = 2
	params.Charset = "UTF-8"
	
	// Mở connection pool
	db, err := sql.Open("godror", params.StringWithPassword())
	if err != nil {
		return nil, fmt.Errorf("lỗi kết nối đến Oracle: %w", err)
	}

	// Kiểm tra kết nối
	if err = db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("lỗi kiểm tra kết nối: %w", err)
	}

	return db, nil
}

func createPool(pgDSN string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(pgDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	config.MaxConns = 20
	config.MinConns = 5
	config.MaxConnLifetime = time.Hour
	config.MaxConnIdleTime = 30 * time.Minute
	config.HealthCheckPeriod = 1 * time.Minute

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to create pool: %w", err)
	}

	return pool, nil
}

func main() {
	defer timeTrack(time.Now(), "migration step")

	// Lấy các biến môi trường cần thiết
	oracleDSN := os.Getenv("ORACLE_DSN")
	pgDSN := os.Getenv("PG_DSN")
	tableName := os.Getenv("TABLE_NAME")

	if oracleDSN == "" || pgDSN == "" || tableName == "" {
		log.Fatal("Vui lòng set biến môi trường ORACLE_DSN, PG_DSN và TABLE_NAME")
	}

	// Các biến để xác định chế độ phân vùng:
	// Nếu PARTITION_BY = "rownum" -> phân chia theo rownum
	// Nếu PARTITION_BY khác và PARTITION_COLUMN được set -> phân chia theo cột
	partitionBy := os.Getenv("PARTITION_BY")
	partitionColumn := os.Getenv("PARTITION_COLUMN")
	filterWhere := os.Getenv("FILTER")
	if strings.Trim(filterWhere," ") =="" {
		filterWhere = " AND 1=1 "
	}else{
		filterWhere = " AND " + filterWhere
	}
	// Kết nối đến Oracle
	oracleDB, err := createOraclePool(oracleDSN,"Asia/Tokyo")
	if err != nil {
		log.Fatalf("Error creating Oracle pool: %v", err)
	}
	defer oracleDB.Close()

	// Kết nối đến PostgreSQL
	pool, err := createPool(pgDSN)
	if err != nil {
		log.Fatalf("Error creating PostgreSQL pool: %v", err)
	}
	defer pool.Close()

	// Lấy danh sách tên cột từ Oracle (dùng query dummy)
	dummyQuery := fmt.Sprintf("SELECT * FROM %s WHERE 1=0", tableName)
	dummyRows, err := oracleDB.Query(dummyQuery)
	if err != nil {
		log.Fatalf("Error executing dummy query: %v", err)
	}
	columns, err := dummyRows.Columns()
	if err != nil {
		log.Fatalf("Error getting columns: %v", err)
	}
	dummyRows.Close()

	log.Printf("Columns ban đầu: %v", columns)
	for i, col := range columns {
		columns[i] = strings.ToLower(col)
	}
	log.Printf("Columns (sau khi chuyển về chữ thường): %v", columns)

	// Tạo channel để truyền các dòng dữ liệu
	dataChan := make(chan []interface{}, 100000)

	// Khởi tạo worker pool cho PostgreSQL (ví dụ 8 worker)
	numWorkers := 10
	var workerWg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		workerWg.Add(1)
		go worker(i, pool, tableName, columns, dataChan, &workerWg)
	}

	// Xử lý đọc dữ liệu từ Oracle theo chế độ phân vùng
	if strings.ToLower(partitionBy) == "rownum" {
		// Phân vùng theo rownum
		var totalRows int64
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE 1=1 %s", tableName , filterWhere)
		if err := oracleDB.QueryRow(countQuery).Scan(&totalRows); err != nil {
			log.Fatalf("Lỗi lấy tổng số row: %v", err)
		}
		log.Printf("Tổng số rows: %d", totalRows)

		// Lấy số lượng partition từ biến môi trường (mặc định 8)
		partitionCount := 10
		if s := os.Getenv("PARTITION_COUNT"); s != "" {
			if cnt, err := strconv.Atoi(s); err == nil && cnt > 0 {
				partitionCount = cnt
			}
		}

		// Tính kích thước mỗi partition (làm tròn lên)
		rangeSize := totalRows / int64(partitionCount)
		if totalRows%int64(partitionCount) != 0 {
			rangeSize++
		}

		// Chuẩn bị danh sách các cột dưới dạng chuỗi để tránh query trả thêm cột rownum
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
				defer partitionWg.Done()
				query := fmt.Sprintf("SELECT %s FROM (SELECT t.*, rownum rnum FROM %s t WHERE 1=1 %s) WHERE rnum BETWEEN %d AND %d", colList, tableName, filterWhere, startRow, endRow)
				rows, err := oracleDB.Query(query, godror.PrefetchCount(FETCHED_NUM_ROWS), godror.FetchArraySize(FETCHED_NUM_ROWS))
				if err != nil {
					log.Printf("Partition %d: lỗi query: %v", partitionId, err)
					return
				}
				defer rows.Close()

				colCount := len(columns)
				count := 0
				// Tạo sẵn slice con trỏ để scan và tái sử dụng
				scanArgs := make([]interface{}, colCount)
				for i := range scanArgs {
					scanArgs[i] = new(interface{})
				}

				for rows.Next() {
					if err := rows.Scan(scanArgs...); err != nil {
						log.Printf("Partition %d: lỗi scan row: %v", partitionId, err)
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
						log.Printf("Partition %d: Đã đọc %d rows", partitionId, count)
					}
				}
				if err := rows.Err(); err != nil {
					log.Printf("Partition %d: lỗi khi lặp qua rows: %v", partitionId, err)
				}
				log.Printf("Partition %d: hoàn tất đọc, tổng số rows: %d", partitionId, count)
			}(startRow, endRow, i)
		}
		partitionWg.Wait()
	} else if partitionColumn != "" {
		// Phân vùng theo cột (như code cũ)
		var minVal, maxVal int64
		queryMinMax := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", partitionColumn, partitionColumn, tableName)
		row := oracleDB.QueryRow(queryMinMax)
		if err := row.Scan(&minVal, &maxVal); err != nil {
			log.Fatalf("Error scanning min/max từ Oracle: %v", err)
		}
		log.Printf("Giá trị nhỏ nhất và lớn nhất của %s: %d - %d", partitionColumn, minVal, maxVal)

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

				query := fmt.Sprintf("SELECT * FROM %s WHERE %s BETWEEN %d AND %d", tableName, partitionColumn, startVal, endVal)
				//rows, err := oracleDB.Query(query)
				rows, err := oracleDB.Query(query, godror.PrefetchCount(FETCHED_NUM_ROWS), godror.FetchArraySize(FETCHED_NUM_ROWS))
				if err != nil {
					log.Printf("Partition %d: lỗi thực hiện query: %v", partitionId, err)
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
						log.Printf("Partition %d: lỗi scan row: %v", partitionId, err)
						continue
					}

					rowData := make([]interface{}, colCount)
					for i, ptr := range scanArgs {
						rowData[i] = *(ptr.(*interface{}))
					}

					dataChan <- rowData
					count++
					if count%100000 == 0 {
						log.Printf("Partition %d: Đã đọc %d rows", partitionId, count)
					}
				}
				if err := rows.Err(); err != nil {
					log.Printf("Partition %d: lỗi khi lặp qua rows: %v", partitionId, err)
				}
				log.Printf("Partition %d: hoàn tất đọc, tổng số rows: %d", partitionId, count)
			}(startVal, endVal, i)
		}
		partitionWg.Wait()
	} else {
		// Nếu không có chế độ phân vùng, đọc dữ liệu bằng một query duy nhất
		query := fmt.Sprintf("SELECT * FROM %s WHERE 1=1 %s", tableName, filterWhere)
		// rows, err := oracleDB.Query(query)
		rows, err := oracleDB.Query(query, godror.PrefetchCount(FETCHED_NUM_ROWS), godror.FetchArraySize(FETCHED_NUM_ROWS))
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
			if count%100000 == 0 {
				log.Printf("Đã đọc %d rows từ Oracle", count)
			}
		}
		if err = rows.Err(); err != nil {
			log.Fatalf("Error iterating over rows: %v", err)
		}
	}

	// Đóng channel sau khi đọc xong
	close(dataChan)

	// Chờ các worker PostgreSQL hoàn tất việc chèn dữ liệu
	workerWg.Wait()

	log.Printf("Hoàn tất di chuyển dữ liệu.")
}
