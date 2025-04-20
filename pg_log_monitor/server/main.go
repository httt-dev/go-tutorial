package main

import (
	"encoding/csv"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hpcloud/tail"
	"github.com/joho/godotenv"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

var clients = make(map[*websocket.Conn]bool)

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade error:", err)
		return
	}
	defer conn.Close()

	clients[conn] = true
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			log.Println("Read error:", err)
			delete(clients, conn)
			break
		}
	}
}

// getLatestLogFile tìm file log mới nhất trong thư mục dựa trên thời gian sửa đổi
func getLatestLogFile(logDir string) (string, error) {
	files, err := os.ReadDir(logDir)
	if err != nil {
		return "", err
	}

	type fileInfo struct {
		name    string
		modTime time.Time
	}

	var logFiles []fileInfo
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "postgresql-") && strings.HasSuffix(file.Name(), ".csv.csv") {
			info, err := file.Info()
			if err != nil {
				continue
			}
			logFiles = append(logFiles, fileInfo{name: file.Name(), modTime: info.ModTime()})
		}
	}

	if len(logFiles) == 0 {
		return "", os.ErrNotExist
	}

	// Sắp xếp file theo thời gian sửa đổi, file mới nhất lên đầu
	sort.Slice(logFiles, func(i, j int) bool {
		return logFiles[i].modTime.After(logFiles[j].modTime)
	})

	return filepath.Join(logDir, logFiles[0].name), nil
}

// cleanQuery loại bỏ tiền tố và chuẩn hóa truy vấn
func cleanQuery(query string) string {
	// Loại bỏ dấu ngoặc kép
	query = strings.Trim(query, "\"")
	// Loại bỏ tiền tố như "譁・ " hoặc các ký tự tương tự
	if idx := strings.Index(query, "文: "); idx != -1 {
		query = strings.TrimSpace(query[idx+len("文: "):])
	}
	return query
}

func tailLogFile() {
	// Tải biến môi trường từ file .env
	err := godotenv.Load()
	if err != nil {
		log.Println("Không tìm thấy file .env, sử dụng thư mục log mặc định")
	}

	// Lấy giá trị LOG_DIR từ biến môi trường
	logDir := os.Getenv("LOG_DIR")
	if logDir == "" {
		logDir = "/var/lib/postgresql/data/log" // Giá trị mặc định
	}

	var currentFile string
	for {
		// Tìm file log mới nhất
		latestFile, err := getLatestLogFile(logDir)
		if err != nil {
			log.Println("Error finding log file:", err)
			time.Sleep(1 * time.Minute)
			continue
		}

		// Nếu file mới nhất khác file hiện tại, chuyển sang theo dõi file mới
		if latestFile != currentFile {
			currentFile = latestFile
			log.Println("Tailing new log file:", currentFile)
		}

		// Mở file log
		file, err := os.Open(currentFile)
		if err != nil {
			log.Println("Error opening log file:", err)
			time.Sleep(1 * time.Minute)
			continue
		}

		// Tạo CSV reader
		csvReader := csv.NewReader(file)
		csvReader.FieldsPerRecord = -1 // Cho phép số cột linh hoạt
		csvReader.TrimLeadingSpace = true
		csvReader.LazyQuotes = true // Cho phép xử lý dấu ngoặc kép không chuẩn

		// Theo dõi file để phát hiện nội dung mới
		t, err := tail.TailFile(currentFile, tail.Config{
			Follow:   true,
			ReOpen:   true,
			Poll:     true,
			Location: &tail.SeekInfo{Offset: 0, Whence: io.SeekEnd}, // Bắt đầu từ cuối file
		})
		if err != nil {
			log.Println("Error tailing log file:", err)
			file.Close()
			time.Sleep(1 * time.Minute)
			continue
		}

		for line := range t.Lines {
			// Đọc từng bản ghi CSV từ file
			fields, err := csvReader.Read()
			if err != nil {
				if err != io.EOF {
					log.Printf("Error parsing CSV record: %v\nLine: %s\n", err, line.Text)
				}
				continue
			}

			if len(fields) > 13 {
				// Lấy và làm sạch truy vấn
				query := cleanQuery(fields[13])
				// Loại bỏ các truy vấn hệ thống liên quan đến pg_catalog
				if strings.Contains(strings.ToLower(query), "pg_catalog") {
					continue
				}
				// Chỉ giữ các truy vấn SELECT, INSERT, UPDATE, DELETE
				upperQuery := strings.ToUpper(query)
				if strings.HasPrefix(upperQuery, "SELECT") ||
					strings.HasPrefix(upperQuery, "INSERT") ||
					strings.HasPrefix(upperQuery, "UPDATE") ||
					strings.HasPrefix(upperQuery, "DELETE") {
					// (Tùy chọn) Loại bỏ truy vấn từ pgAdmin nếu cần
					if len(fields) > 14 && strings.Contains(fields[14], "pgAdmin 4") {
						continue
					}
					log.Println("Query:", query)
					for client := range clients {
						err := client.WriteMessage(websocket.TextMessage, []byte(query))
						if err != nil {
							log.Println("Write error:", err)
							client.Close()
							delete(clients, client)
						}
					}
				}
			}
		}

		file.Close()
		t.Cleanup()
	}
}

func main() {
	go tailLogFile()

	http.HandleFunc("/ws", handleWebSocket)
	http.Handle("/", http.FileServer(http.Dir(".")))

	log.Println("Server started on :8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}
