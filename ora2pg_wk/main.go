package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	_ "github.com/godror/godror" // Driver Oracle
	_ "github.com/lib/pq"        // Driver PostgreSQL

	"github.com/joho/godotenv"
	"github.com/lib/pq"
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Printf("Không tìm thấy file .env, sử dụng biến môi trường hệ thống: %v", err)
	}
}

// insertBatch thực hiện chèn một batch (nhiều dòng) vào PostgreSQL sử dụng COPY
func insertBatch(db *sql.DB, tableName string, columns []string, batch [][]interface{}) error {
	// Mở transaction
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("tx begin error: %w", err)
	}

	// Chuẩn bị statement COPY với table name và danh sách cột
	stmt, err := tx.Prepare(pq.CopyIn(tableName, columns...))
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare copyin error: %w", err)
	}

	// Thực hiện COPY cho từng dòng trong batch
	for _, row := range batch {
		_, err = stmt.Exec(row...)
		if err != nil {
			stmt.Close()
			tx.Rollback()
			return fmt.Errorf("stmt exec error: %w", err)
		}
	}

	// Kết thúc COPY
	_, err = stmt.Exec()
	if err != nil {
		stmt.Close()
		tx.Rollback()
		return fmt.Errorf("final stmt exec error: %w", err)
	}

	// Đóng statement và commit transaction
	if err := stmt.Close(); err != nil {
		tx.Rollback()
		return fmt.Errorf("stmt close error: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("tx commit error: %w", err)
	}

	return nil
}

// Worker sử dụng chung connection pool (db) cho PostgreSQL
func worker(workerId int, db *sql.DB, tableName string, columns []string, dataChan <-chan []interface{}, wg *sync.WaitGroup) {
	defer wg.Done()

	batchSize := 10000 // Số dòng mỗi batch (có thể điều chỉnh)
	batch := make([][]interface{}, 0, batchSize)

	for row := range dataChan {
		batch = append(batch, row)
		if len(batch) >= batchSize {
			if err := insertBatch(db, tableName, columns, batch); err != nil {
				log.Printf("Worker %d: error inserting batch: %v", workerId, err)
			}
			// Reset batch
			batch = batch[:0]
		}
	}

	// Nếu còn dư dữ liệu trong batch, chèn chúng
	if len(batch) > 0 {
		if err := insertBatch(db, tableName, columns, batch); err != nil {
			log.Printf("Worker %d: error inserting last batch: %v", workerId, err)
		}
	}

	log.Printf("Worker %d: finished processing", workerId)
}

func main() {
	// Lấy chuỗi kết nối từ biến môi trường
	oracleDSN := os.Getenv("ORACLE_DSN") // Đã load từ file .env
	pgDSN := os.Getenv("PG_DSN")         // Đã load từ file .env

	if oracleDSN == "" || pgDSN == "" {
		log.Fatal("Vui lòng set biến môi trường ORACLE_DSN và PG_DSN")
	}

	tableName := os.Getenv("TABLE_NAME")
	if tableName == "" {
		log.Fatal("Vui lòng set biến môi trường TABLE_NAME")
	}

	// Kết nối đến Oracle
	oracleDB, err := sql.Open("godror", oracleDSN)
	if err != nil {
		log.Fatalf("Error connecting to Oracle: %v", err)
	}
	defer oracleDB.Close()

	// Kết nối đến PostgreSQL (dạng pool) chỉ 1 lần duy nhất
	pgDB, err := sql.Open("postgres", pgDSN)
	if err != nil {
		log.Fatalf("Error connecting to PostgreSQL: %v", err)
	}
	defer pgDB.Close()

	// Thực hiện query dữ liệu từ Oracle (lấy tất cả các cột)
	query := fmt.Sprintf("SELECT * FROM %s  where rownum <= 500000", tableName)
	rows, err := oracleDB.Query(query)
	if err != nil {
		log.Fatalf("Error executing query on Oracle: %v", err)
	}
	defer rows.Close()

	// Lấy danh sách tên cột từ Oracle
	columns, err := rows.Columns()
	if err != nil {
		log.Fatalf("Error getting columns from Oracle query: %v", err)
	}
	log.Printf("Columns: %v", columns)

	for i, col := range columns {
		columns[i] = strings.ToLower(col)
	}
	log.Printf("Columns (sau khi chuyển về chữ thường): %v", columns)
	
	// Tạo channel để truyền các dòng dữ liệu (mỗi dòng là []interface{})
	dataChan := make(chan []interface{}, 1000)

	// Khởi tạo worker pool (ví dụ 8 worker) và chia sẻ pgDB cho các worker
	numWorkers := 8
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(i, pgDB, tableName, columns, dataChan, &wg)
	}

	// Đọc dữ liệu từ Oracle và gửi vào channel
	count := 0
	colCount := len(columns)
	for rows.Next() {
		// Tạo slice chứa con trỏ cho mỗi cột
		valPtrs := make([]interface{}, colCount)
		for i := 0; i < colCount; i++ {
			var dummy interface{}
			valPtrs[i] = &dummy
		}

		// Quét dữ liệu của dòng vào slice con trỏ
		if err := rows.Scan(valPtrs...); err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}

		// Chuyển đổi các giá trị từ con trỏ sang giá trị thực
		row := make([]interface{}, colCount)
		for i, ptr := range valPtrs {
			row[i] = *(ptr.(*interface{}))
		}

		// Gửi dòng dữ liệu vào channel
		dataChan <- row
		count++
		if count%10000 == 0 {
			log.Printf("Đã đọc %d rows từ Oracle", count)
		}
	}
	if err = rows.Err(); err != nil {
		log.Fatalf("Error iterating over rows: %v", err)
	}

	// Đóng channel sau khi đọc xong
	close(dataChan)

	// Chờ các worker hoàn thành công việc
	wg.Wait()
	log.Printf("Hoàn tất di chuyển dữ liệu. Tổng số rows: %d", count)
}
