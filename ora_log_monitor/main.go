package main

import (
	"database/sql"
	"encoding/json"
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

// clientConn holds WebSocket connection and associated userhost
type clientConn struct {
	conn     *websocket.Conn
	userhost string
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

var clients = make(map[*websocket.Conn]clientConn)

// AuditMessage defines the structure of the JSON message sent to WebSocket clients
type AuditMessage struct {
	Timestamp  string  `json:"timestamp"`
	Userhost   string  `json:"userhost"`
	Username   string  `json:"username"`
	ActionName string  `json:"actionname"`
	SQL        string  `json:"sql"`
	SCN        string  `json:"scn"`
	ErrorMsg   *string `json:"errormsg"`
}

// handleWebSocket upgrades HTTP connection to WebSocket and associates with userhost
func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Extract userhost from URL path (e.g., /ws/HOA_PC)
	pathParts := strings.Split(r.URL.Path, "/")
	userhost := ""
	if len(pathParts) > 2 {
		userhost = strings.ToUpper(pathParts[len(pathParts)-1]) // Normalize to uppercase for consistency
	}
	if userhost == "" {
		http.Error(w, "Userhost not provided in URL path", http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade error for userhost", userhost, ":", err)
		return
	}

	// Store connection with associated userhost
	clients[conn] = clientConn{conn: conn, userhost: userhost}
	log.Printf("New WebSocket connection for userhost: %s", userhost)

	defer func() {
		conn.Close()
		delete(clients, conn)
		log.Printf("Closed WebSocket connection for userhost: %s", userhost)
	}()

	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			log.Println("Read error for userhost", userhost, ":", err)
			delete(clients, conn)
			break
		}
	}
}

// reconstructQuery replaces bind variables with their values
func reconstructQuery(sqlText, sqlBind string) (string, error) {
	if sqlBind == "" || !strings.Contains(sqlText, ":") {
		return sqlText, nil
	}

	// Load Asia/Bangkok timezone for datetime parsing
	bangkokLoc, err := time.LoadLocation("Asia/Bangkok")
	if err != nil {
		return "", err
	}

	// Regex to detect Oracle datetime format (e.g., 04-JUN-25 07.27.45.379000000 PM)
	datetimeRegex := regexp.MustCompile(`^\d{2}-[A-Z]{3}-\d{2} \d{2}\.\d{2}\.\d{2}\.\d{6,9} (AM|PM)$`)

	// 1. Lấy danh sách bind variables
	bindVarRegex := regexp.MustCompile(`:([a-zA-Z_][a-zA-Z0-9_]*|\d+)`)
	matches := bindVarRegex.FindAllStringSubmatch(sqlText, -1)

	var bindVars []string
	seen := map[string]bool{}
	for _, match := range matches {
		if len(match) > 1 && !seen[match[1]] {
			bindVars = append(bindVars, match[1])
			seen[match[1]] = true
		}
	}

	// 2. Duyệt thủ công các đoạn bind
	bindHeaderRegex := regexp.MustCompile(`#\d+\(\d+\):`)
	locs := bindHeaderRegex.FindAllStringIndex(sqlBind, -1)

	bindValues := make(map[int]string)
	for i, loc := range locs {
		start := loc[0]
		end := len(sqlBind)
		if i+1 < len(locs) {
			end = locs[i+1][0]
		}
		full := sqlBind[start:end]
		// full: #1(31):04-JUN-25 07.27.45.379000000 PM
		parts := strings.SplitN(full, ":", 2)
		if len(parts) != 2 {
			continue
		}
		header := parts[0] // #1(31)
		value := strings.TrimSpace(parts[1])

		numRe := regexp.MustCompile(`#(\d+)\(`)
		numMatch := numRe.FindStringSubmatch(header)
		if len(numMatch) == 2 {
			if idx, err := strconv.Atoi(numMatch[1]); err == nil {
				if value == "" {
					bindValues[idx] = "NULL" // Handle empty bind value as NULL
				} else if datetimeRegex.MatchString(value) {
					// Parse Oracle datetime (e.g., 04-JUN-25 07.27.45.379000000 PM)
					var parsedTime time.Time
					// Try with nanoseconds (9 digits)
					parsedTime, err = time.ParseInLocation("02-JAN-06 03.04.05.999999999 PM", value, bangkokLoc)
					if err != nil {
						// Try with microseconds (6 digits)
						parsedTime, err = time.ParseInLocation("02-JAN-06 03.04.05.999999 PM", value, bangkokLoc)
						if err != nil {
							log.Printf("Error parsing datetime %s: %v", value, err)
							bindValues[idx] = value // Fallback to original value if parsing fails
							continue
						}
					}
					// Format to 2025-06-04 19:27:45.379
					bindValues[idx] = parsedTime.Format("2006-01-02 15:04:05.999")
				} else {
					bindValues[idx] = value
				}
			}
		}
	}

	// 3. Thay thế các giá trị vào SQL
	result := sqlText
	for i, bindName := range bindVars {
		bindIndex := i + 1
		value, exists := bindValues[bindIndex]
		if !exists {
			continue // Skip if no bind value found for this index
		}
		if value == "NULL" {
			result = strings.Replace(result, ":"+bindName, "NULL", 1)
		} else {
			// Quote string-like values
			if !(strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'")) {
				value = "'" + value + "'"
			}
			result = strings.Replace(result, ":"+bindName, value, 1)
		}
	}

	return result, nil
}

// tailAuditTrail queries DBA_AUDIT_TRAIL and sends results to WebSocket clients
func tailAuditTrail() {
	// Load environment variables
	err := godotenv.Load()
	if err != nil {
		log.Println("No .env file found, using default connection")
	}

	// Connect to Oracle (ORCLPDB1)
	db, err := sql.Open("godror", "AIPDEV/Abc12345@192.168.1.116:1522/ORCLPDB1?timezone=Asia/Bangkok")
	if err != nil {
		log.Fatalf("Error connecting to Oracle: %v", err)
	}
	defer db.Close()

	// Load Asia/Bangkok timezone for Go
	bangkokLoc, err := time.LoadLocation("Asia/Bangkok")
	if err != nil {
		log.Fatalf("Error loading Asia/Bangkok timezone: %v", err)
	}

	// Initialize lastTimestamp to current time
	lastTimestamp := time.Now().In(bangkokLoc)
	lastTimestampStr := lastTimestamp.Format("2006-01-02 15:04:05")
	log.Printf("Initial lastTimestamp: %v, String: %s", lastTimestamp, lastTimestampStr)

	for {
		// Query DBA_AUDIT_TRAIL for SELECT, INSERT, UPDATE, DELETE
		rows, err := db.Query(`
            SELECT DISTINCT TO_CHAR(TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS TIMESTAMP, USERHOST, USERNAME, ACTION_NAME, SQL_TEXT, SQL_BIND, SCN
            FROM DBA_AUDIT_TRAIL
            WHERE TO_CHAR(TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') > :1
              AND ACTION_NAME IN ('SELECT', 'INSERT', 'UPDATE', 'DELETE')
              AND OWNER IN ('AIPDEV')
              AND (SQL_TEXT NOT LIKE 'SELECT%FROM V$%' AND SQL_TEXT NOT LIKE 'SELECT%FROM DBA_%')
            ORDER BY SCN`, lastTimestampStr)
		if err != nil {
			log.Printf("Error querying audit trail: %v", err)
			// Send error message to all relevant clients
			errorMsg := err.Error()
			message := AuditMessage{
				Timestamp:  time.Now().In(bangkokLoc).Format("2006-01-02 15:04:05"),
				Userhost:   "",
				Username:   "",
				ActionName: "",
				SQL:        "",
				SCN:        "",
				ErrorMsg:   &errorMsg,
			}
			messageJSON, _ := json.Marshal(message)
			for client, clientInfo := range clients {
				err := client.WriteMessage(websocket.TextMessage, messageJSON)
				if err != nil {
					log.Println("Write error for userhost", clientInfo.userhost, ":", err)
					client.Close()
					delete(clients, client)
				}
			}
			time.Sleep(3 * time.Second)
			continue
		}

		rowCount := 0
		for rows.Next() {
			var timestampStr string
			var timestamp time.Time
			var userhost, username, actionname, sqlText, sqlBind, scn sql.NullString
			err := rows.Scan(&timestampStr, &userhost, &username, &actionname, &sqlText, &sqlBind, &scn)
			if err != nil {
				log.Printf("Error scanning audit row: %v", err)
				// Send error message to all relevant clients
				errorMsg := err.Error()
				message := AuditMessage{
					Timestamp:  time.Now().In(bangkokLoc).Format("2006-01-02 15:04:05"),
					Userhost:   "",
					Username:   "",
					ActionName: "",
					SQL:        "",
					SCN:        "",
					ErrorMsg:   &errorMsg,
				}
				messageJSON, _ := json.Marshal(message)
				for client, clientInfo := range clients {
					err := client.WriteMessage(websocket.TextMessage, messageJSON)
					if err != nil {
						log.Println("Write error for userhost", clientInfo.userhost, ":", err)
						client.Close()
						delete(clients, client)
					}
				}
				continue
			}

			// Parse timestampStr back to time.Time for updating lastTimestamp
			timestamp, err = time.ParseInLocation("2006-01-02 15:04:05", timestampStr, bangkokLoc)
			if err != nil {
				log.Printf("Error parsing timestampStr %s: %v", timestampStr, err)
				// Send error message to all relevant clients
				errorMsg := err.Error()
				message := AuditMessage{
					Timestamp:  time.Now().In(bangkokLoc).Format("2006-01-02 15:04:05"),
					Userhost:   "",
					Username:   "",
					ActionName: "",
					SQL:        "",
					SCN:        "",
					ErrorMsg:   &errorMsg,
				}
				messageJSON, _ := json.Marshal(message)
				for client, clientInfo := range clients {
					err := client.WriteMessage(websocket.TextMessage, messageJSON)
					if err != nil {
						log.Println("Write error for userhost", clientInfo.userhost, ":", err)
						client.Close()
						delete(clients, client)
					}
				}
				continue
			}

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
			var errorMsg *string
			if sqlBind.Valid {
				var err error
				query, err = reconstructQuery(sqlText.String, sqlBind.String)
				if err != nil {
					log.Printf("Error reconstructing query: %v", err)
					errStr := err.Error()
					errorMsg = &errStr
				}
			}

			rowCount++
			// Create JSON message
			message := AuditMessage{
				Timestamp:  timestampStr,
				Userhost:   userhost.String,
				Username:   username.String,
				ActionName: actionname.String,
				SQL:        query,
				SCN:        scn.String,
				ErrorMsg:   errorMsg,
			}
			messageJSON, err := json.Marshal(message)
			if err != nil {
				log.Printf("Error marshaling JSON: %v", err)
				continue
			}

			// Send JSON message to WebSocket clients with matching userhost
			log.Println("Audit Message:", string(messageJSON))
			for client, clientInfo := range clients {
				log.Printf("Sending message to userhost: %s", clientInfo.userhost)
				if strings.EqualFold(clientInfo.userhost, userhost.String) {
					err := client.WriteMessage(websocket.TextMessage, messageJSON)
					if err != nil {
						log.Println("Write error for userhost", clientInfo.userhost, ":", err)
						client.Close()
						delete(clients, client)
					}
				}
			}
		}
		rows.Close()

		// Log current lastTimestamp for debugging
		log.Printf("Current lastTimestamp: %v", lastTimestamp)

		// Wait before next query
		time.Sleep(5 * time.Second)
	}
}

func main() {
	go tailAuditTrail()

	// Handle WebSocket connections with userhost in path (e.g., /ws/HOA_PC)
	http.HandleFunc("/ws/", handleWebSocket)

	// Serve index.html for all other requests
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "index.html")
	})

	log.Println("Server started on :8091")
	log.Fatal(http.ListenAndServe(":8091", nil))
}
