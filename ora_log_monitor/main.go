package main

import (
	"database/sql"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	_ "github.com/godror/godror" // Oracle driver
	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

var clients = make(map[*websocket.Conn]bool)

// handleWebSocket upgrades HTTP connection to WebSocket
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

// reconstructQuery replaces bind variables with their values
func reconstructQuery(sqlText, sqlBind string) string {
	if sqlBind == "" || !strings.Contains(sqlText, ":") {
		return sqlText
	}

	// 1. Tìm danh sách bind theo tên từ câu SQL (ví dụ :start, :end)
	bindNameRegex := regexp.MustCompile(`:([a-zA-Z_][a-zA-Z0-9_]*)`)
	matches := bindNameRegex.FindAllStringSubmatch(sqlText, -1)

	var bindNames []string
	seen := map[string]bool{}
	for _, match := range matches {
		if len(match) > 1 && !seen[match[1]] {
			bindNames = append(bindNames, match[1])
			seen[match[1]] = true
		}
	}

	// 2. Tách các bind value theo thứ tự: #1(4):'abc' -> 1:'abc'
	bindValues := make(map[int]string)
	bindParts := strings.Fields(sqlBind)
	for _, part := range bindParts {
		if strings.Contains(part, ":") {
			parts := strings.SplitN(part, ":", 2)
			if len(parts) != 2 {
				continue
			}
			prefix := parts[0] // #1(4)
			value := parts[1]  // 'abc'

			// Extract number from #<num>(...)
			re := regexp.MustCompile(`#(\d+)\(`)
			numMatch := re.FindStringSubmatch(prefix)
			if len(numMatch) == 2 {
				index := numMatch[1]
				// Convert index string to int
				if i, err := strconv.Atoi(index); err == nil {
					bindValues[i] = value
				}
			}
		}
	}

	// 3. Gán giá trị vào đúng tên bind theo thứ tự
	result := sqlText
	for i, name := range bindNames {
		value := bindValues[i+1] // vì bind index bắt đầu từ 1
		if value != "" {
			result = strings.Replace(result, ":"+name, value, 1)
		}
	}

	return result
}

// tailAuditTrail queries DBA_AUDIT_TRAIL and sends results to WebSocket clients
func tailAuditTrail() {
	// Load environment variables
	err := godotenv.Load()
	if err != nil {
		log.Println("No .env file found, using default connection")
	}

	// Connect to Oracle (ORCLPDB1)
	db, err := sql.Open("godror", "AIPBODEV/Abc12345@localhost:1522/ORCLPDB1?timezone=Asia/Bangkok")
	if err != nil {
		log.Fatalf("Error connecting to Oracle: %v", err)
	}
	defer db.Close()

	// // Set session to ORCLPDB1
	// _, err = db.Exec("ALTER SESSION SET CONTAINER=ORCLPDB1")
	// if err != nil {
	// 	log.Fatalf("Error setting container to ORCLPDB1: %v", err)
	// }

	// Load Asia/Bangkok timezone for Go
	bangkokLoc, err := time.LoadLocation("Asia/Bangkok")
	if err != nil {
		log.Fatalf("Error loading Asia/Bangkok timezone: %v", err)
	}

	// Initialize lastTimestamp to a fixed past date
	lastTimestamp, err := time.ParseInLocation("2006-01-02 15:04:05", "2025-04-27 00:00:00", bangkokLoc)
	if err != nil {
		log.Fatalf("Error parsing initial lastTimestamp: %v", err)
	}
	lastTimestampStr := lastTimestamp.Format("2006-01-02 15:04:05")
	log.Printf("Initial lastTimestamp: %v, String: %s", lastTimestamp, lastTimestampStr)

	for {

		// Query DBA_AUDIT_TRAIL for SELECT, INSERT, UPDATE, DELETE
		rows, err := db.Query(`
            SELECT TO_CHAR(TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS TIMESTAMP, USERHOST,USERNAME, SQL_TEXT, SQL_BIND
            FROM DBA_AUDIT_TRAIL
            WHERE TO_CHAR(TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') > :1
              AND ACTION_NAME IN ('SELECT', 'INSERT', 'UPDATE', 'DELETE')
              AND OWNER IN ('AIPBODEV')
              AND (SQL_TEXT NOT LIKE 'SELECT%FROM V$%' AND SQL_TEXT NOT LIKE 'SELECT%FROM DBA_%')
            ORDER BY TIMESTAMP`, lastTimestampStr)
		if err != nil {
			log.Printf("Error querying audit trail: %v", err)
			time.Sleep(3 * time.Second)
			continue
		}

		// log.Println("Checking Audit Trail data...")
		rowCount := 0
		for rows.Next() {
			var timestampStr string
			var timestamp time.Time
			var username, sqlText, sqlBind sql.NullString
			err := rows.Scan(&timestampStr, &username, &sqlText, &sqlBind)
			if err != nil {
				log.Printf("Error scanning audit row: %v", err)
				continue
			}

			// Parse timestampStr back to time.Time for updating lastTimestamp
			timestamp, err = time.ParseInLocation("2006-01-02 15:04:05", timestampStr, bangkokLoc)
			if err != nil {
				log.Printf("Error parsing timestampStr %s: %v", timestampStr, err)
				continue
			}

			// Log the raw TIMESTAMP for debugging
			// log.Printf("Raw TIMESTAMP String: %s, Parsed: %v", timestampStr, timestamp)

			// Update last timestamp
			if timestamp.After(lastTimestamp) {
				lastTimestamp = timestamp
				lastTimestampStr = timestampStr
			}

			// Skip system queries
			if sqlText.Valid && (strings.Contains(strings.ToLower(sqlText.String), "v$") ||
				strings.Contains(strings.ToLower(sqlText.String), "dba_")) {
				continue
			}

			// Reconstruct query with bind values
			query := sqlText.String
			if sqlBind.Valid {
				query = reconstructQuery(sqlText.String, sqlBind.String)
			}

			rowCount++
			// Send query to WebSocket clients
			log.Println("Audit Query:", query)
			for client := range clients {
				err := client.WriteMessage(websocket.TextMessage, []byte(query))
				if err != nil {
					log.Println("Write error:", err)
					client.Close()
					delete(clients, client)
				}
			}
		}
		// log.Printf("Audit Trail returned %d rows", rowCount)
		rows.Close()

		// Log current lastTimestamp for debugging
		log.Printf("Current lastTimestamp: %v", lastTimestamp)

		// Wait before next query
		time.Sleep(5 * time.Second)
	}
}

func main() {
	go tailAuditTrail()

	http.HandleFunc("/ws", handleWebSocket)
	http.Handle("/", http.FileServer(http.Dir(".")))

	log.Println("Server started on :8091")
	log.Fatal(http.ListenAndServe(":8091", nil))
}
