package main

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hpcloud/tail"
	"github.com/joho/godotenv"
)

// LogEntry represents the structure of a JSON log entry
type LogEntry struct {
	Timestamp       string `json:"timestamp"`
	User            string `json:"user"`
	Dbname          string `json:"dbname"`
	RemoteHost      string `json:"remote_host"`
	Message         string `json:"message"`
	Detail          string `json:"detail"`
	ApplicationName string `json:"application_name"`
	Pid             int    `json:"pid"`
	SessionId       string `json:"session_id"`
	LineNum         int    `json:"line_num"`
	Ps              string `json:"ps"`
	ErrorSeverity   string `json:"error_severity"`
	QueryID         int64  `json:"query_id"`
	Vxid            string `json:"vxid"`
}

// WebSocketMessage represents the JSON message to send to clients
type WebSocketMessage struct {
	Timestamp  string  `json:"timestamp"`
	Userhost   string  `json:"userhost"`
	Username   string  `json:"username"`
	ActionName string  `json:"actionname"`
	SQL        string  `json:"sql"`
	ErrorMsg   *string `json:"errormsg"` // Use pointer to allow nil
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// clients maps userhost (lowercase) to a slice of WebSocket connections
var clients = make(map[string][]*websocket.Conn)
var clientsMu sync.Mutex

// queryCache stores recently processed queries to avoid duplicates
type queryCache struct {
	entries map[string]struct {
		timestamp time.Time
		ps        string
	}
	mutex sync.Mutex
}

var qCache = &queryCache{
	entries: make(map[string]struct {
		timestamp time.Time
		ps        string
	}),
}

// Clean up old cache entries
func (qc *queryCache) cleanup() {
	qc.mutex.Lock()
	defer qc.mutex.Unlock()
	now := time.Now()
	for key, entry := range qc.entries {
		if now.Sub(entry.timestamp) > 100*time.Millisecond { // Cache timeout 100ms
			delete(qc.entries, key)
		}
	}
}

// Check if a query has been processed recently, prioritizing BIND
func (qc *queryCache) isProcessed(sessionID string, pid int, query, ps, vxid string, timestamp time.Time) bool {
	qc.mutex.Lock()
	defer qc.mutex.Unlock()
	key := fmt.Sprintf("%s:%d:%s:%s", sessionID, pid, queryHash(query), vxid)
	if entry, exists := qc.entries[key]; exists {
		// If BIND already processed for this vxid, skip any other ps
		if entry.ps == "BIND" {
			log.Printf("Skipping query (BIND already processed): session_id=%s, pid=%d, vxid=%s, ps=%s, query=%s", sessionID, pid, vxid, ps, query)
			return true
		}
		// If current is BIND, update cache and allow processing
		if ps == "BIND" {
			qc.entries[key] = struct {
				timestamp time.Time
				ps        string
			}{timestamp: timestamp, ps: ps}
			return false
		}
		// If same ps within 100ms, skip
		if entry.ps == ps && timestamp.Sub(entry.timestamp) < 100*time.Millisecond {
			log.Printf("Skipping duplicate query: session_id=%s, pid=%d, vxid=%s, ps=%s, query=%s", sessionID, pid, vxid, ps, query)
			return true
		}
	}
	// New query or new vxid, add to cache
	qc.entries[key] = struct {
		timestamp time.Time
		ps        string
	}{timestamp: timestamp, ps: ps}
	return false
}

// queryHash generates an MD5 hash of the query for cache key
func queryHash(query string) string {
	hash := md5.Sum([]byte(query))
	return hex.EncodeToString(hash[:])
}

// bindParameters replaces $1, $2, etc., with their corresponding values from the detail field
func bindParameters(query, detail string) string {
	if detail == "" {
		log.Printf("No detail provided, returning original query: %s", query)
		return query
	}

	// Extract parameters from detail (e.g., "Parameters: $1 = 'value', $2 = NULL, $3 = ''")
	re := regexp.MustCompile(`\$(\d+)\s*=\s*(NULL|'[^']*'|'')`)
	matches := re.FindAllStringSubmatch(detail, -1)

	// Create a map of parameter index to value
	paramMap := make(map[string]string)
	for _, match := range matches {
		if len(match) == 3 {
			index := match[1]
			value := match[2]
			if value == "NULL" {
				paramMap[index] = "NULL"
			} else {
				paramMap[index] = value // Includes quotes for strings, or '' for empty
			}
			log.Printf("Mapped parameter $%s = %s", index, value)
		}
	}

	// Find the highest parameter number in the query (e.g., $12)
	maxParam := 0
	paramRe := regexp.MustCompile(`\$(\d+)`)
	for _, match := range paramRe.FindAllStringSubmatch(query, -1) {
		if len(match) == 2 {
			if num, err := strconv.Atoi(match[1]); err == nil && num > maxParam {
				maxParam = num
			}
		}
	}
	log.Printf("Highest parameter in query: $%d", maxParam)

	// Replace $1, $2, etc., with their values using regex for precise matching
	resultQuery := query
	for i := 1; i <= maxParam; i++ {
		key := fmt.Sprintf("%d", i)
		placeholder := fmt.Sprintf(`\$%d\b`, i) // \b ensures we match $N exactly
		rePlaceholder := regexp.MustCompile(placeholder)
		if value, exists := paramMap[key]; exists {
			log.Printf("Before replacing %s with %s: %s", placeholder, value, resultQuery)
			resultQuery = rePlaceholder.ReplaceAllString(resultQuery, value)
			log.Printf("After replacing %s with %s: %s", placeholder, value, resultQuery)
		} else {
			log.Printf("Warning: No value found for $%s, replacing with NULL", key)
			resultQuery = rePlaceholder.ReplaceAllString(resultQuery, "NULL")
		}
	}

	// Check for remaining placeholders
	remaining := paramRe.FindAllString(resultQuery, -1)
	if len(remaining) > 0 {
		log.Printf("Warning: Found unprocessed placeholders in final query: %v", remaining)
	}

	log.Printf("Final bound query: %s", resultQuery)
	return resultQuery
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Extract userhost from URL path (e.g., /HOA_PC)
	pathParts := strings.Split(r.URL.Path, "/")
	userhost := ""
	if len(pathParts) > 1 {
		userhost = strings.ToLower(pathParts[len(pathParts)-1]) // Convert to lowercase
	}
	if userhost == "" {
		log.Println("No userhost provided in URL path")
		http.Error(w, "Invalid userhost", http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade error:", err)
		return
	}
	defer conn.Close()

	// Add connection to the clients map under userhost
	clientsMu.Lock()
	clients[userhost] = append(clients[userhost], conn)
	clientsMu.Unlock()
	log.Printf("New client connected for userhost: %s", userhost)

	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			log.Println("Read error:", err)
			// Remove connection from clients
			clientsMu.Lock()
			conns := clients[userhost]
			for i, c := range conns {
				if c == conn {
					clients[userhost] = append(conns[:i], conns[i+1:]...)
					break
				}
			}
			if len(clients[userhost]) == 0 {
				delete(clients, userhost)
			}
			clientsMu.Unlock()
			break
		}
	}
}

// getLatestLogFile finds the latest log file in the directory based on modification time
func getLatestLogFile(logDir string) (string, error) {
	files, err := os.ReadDir(logDir)
	if err != nil {
		return "", fmt.Errorf("failed to read log directory: %w", err)
	}

	type fileInfo struct {
		name    string
		modTime time.Time
	}

	var logFiles []fileInfo
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "postgresql-") && strings.HasSuffix(file.Name(), ".json.json") {
			log.Printf("Processing log file: %s", file.Name())
			info, err := file.Info()
			if err != nil {
				log.Printf("Error getting info for file %s: %v", file.Name(), err)
				continue
			}
			logFiles = append(logFiles, fileInfo{name: file.Name(), modTime: info.ModTime()})
		}
	}

	if len(logFiles) == 0 {
		return "", fmt.Errorf("no log files found in %s", logDir)
	}

	// Sort files by modification time, newest first
	sort.Slice(logFiles, func(i, j int) bool {
		return logFiles[i].modTime.After(logFiles[j].modTime)
	})

	latestFile := filepath.Join(logDir, logFiles[0].name)
	log.Printf("Selected latest log file: %s", latestFile)
	return latestFile, nil
}

// cleanQuery extracts only the SQL query starting with SELECT, INSERT, UPDATE, DELETE, or TRUNCATE
func cleanQuery(query string) string {
	// Define regex to match the start of the SQL query
	re := regexp.MustCompile(`(?i)^\s*(duration:\s*\d+\.\d+\s*ms\s*(bind|execute|parse)\s*(<unnamed>|S_\d+):\s*)?(SELECT|INSERT|UPDATE|DELETE|TRUNCATE)\b.*`)
	matches := re.FindStringSubmatch(query)
	if len(matches) >= 5 {
		// Extract the part starting with SELECT, INSERT, UPDATE, DELETE, or TRUNCATE
		return strings.TrimSpace(matches[4] + query[strings.Index(strings.ToUpper(query), matches[4])+len(matches[4]):])
	}
	// If no match, return the original query trimmed
	return strings.TrimSpace(query)
}

func tailLogFile() {
	// Load environment variables from .env file
	err := godotenv.Load()
	if err != nil {
		log.Println("No .env file found, using default log directory")
	}

	// Get LOG_DIR from environment variables
	logDir := os.Getenv("LOG_DIR")
	if logDir == "" {
		logDir = "/var/lib/pgsql/17/data/log" // Default value
	}
	log.Printf("Using log directory: %s", logDir)

	// Start a goroutine to clean up query cache
	cacheCleanupInterval := 1 * time.Second
	cacheTicker := time.NewTicker(cacheCleanupInterval)
	defer cacheTicker.Stop()

	go func() {
		for range cacheTicker.C {
			qCache.cleanup()
			log.Println("Cleared query cache")
		}
	}()

	var currentFile string
	for {
		// Find the latest log file
		latestFile, err := getLatestLogFile(logDir)
		if err != nil {
			log.Printf("Error finding log file: %v", err)
			time.Sleep(1 * time.Minute)
			continue
		}

		// Switch to new file if different
		if currentFile != latestFile {
			currentFile = latestFile
			log.Printf("Switching to log file: %s", currentFile)
		}

		// Tail the log file
		t, err := tail.TailFile(currentFile, tail.Config{
			Follow:   true,
			ReOpen:   true, // Allow tail to handle file rotation
			Poll:     true,
			Location: &tail.SeekInfo{Offset: 0, Whence: io.SeekEnd},
		})
		if err != nil {
			log.Printf("Error tailing log file %s: %v", currentFile, err)
			time.Sleep(1 * time.Minute)
			continue
		}

		for line := range t.Lines {
			if line.Err != nil {
				log.Printf("Error reading line from %s: %v", currentFile, line.Err)
				continue
			}
			log.Printf("Read line from %s: %s", currentFile, line.Text)

			// Parse JSON log line
			var entry LogEntry
			err := json.Unmarshal([]byte(line.Text), &entry)
			if err != nil {
				log.Printf("Error parsing JSON log: %v\nLine: %s", err, line.Text)
				continue
			}
			log.Printf("Parsed log entry: timestamp=%s, session_id=%s, pid=%d, vxid=%s, ps=%s, detail=%s", entry.Timestamp, entry.SessionId, entry.Pid, entry.Vxid, entry.Ps, entry.Detail)

			// Process only logs from PostgreSQL JDBC Driver
			if entry.ApplicationName != "PostgreSQL JDBC Driver" {
				log.Printf("Skipping log: not from PostgreSQL JDBC Driver")
				continue
			}

			// Process BIND, SELECT, INSERT, UPDATE, DELETE, TRUNCATE entries
			if entry.Ps == "BIND" || entry.Ps == "SELECT" || entry.Ps == "INSERT" || entry.Ps == "UPDATE" || entry.Ps == "DELETE" || entry.Ps == "TRUNCATE" {
				query := cleanQuery(entry.Message)
				log.Printf("Processing query with ps=%s: %s", entry.Ps, query)

				// Parse timestamp
				ts, err := time.Parse("2006-01-02 15:04:05.999 -07", entry.Timestamp)
				if err != nil {
					log.Printf("Error parsing timestamp %s: %v", entry.Timestamp, err)
					ts = time.Now()
				}

				// Check if query was recently processed
				if qCache.isProcessed(entry.SessionId, entry.Pid, query, entry.Ps, entry.Vxid, ts) {
					continue
				}

				// Skip system queries (e.g., pg_catalog, pg_replication_slots)
				if strings.Contains(strings.ToLower(query), "pg_") {
					log.Printf("Skipping system query: %s", query)
					continue
				}

				// Process only SELECT, INSERT, UPDATE, DELETE, TRUNCATE queries
				upperQuery := strings.ToUpper(query)
				if strings.HasPrefix(upperQuery, "SELECT") ||
					strings.HasPrefix(upperQuery, "INSERT") ||
					strings.HasPrefix(upperQuery, "UPDATE") ||
					strings.HasPrefix(upperQuery, "DELETE") ||
					strings.HasPrefix(upperQuery, "TRUNCATE") {
					// Bind parameters to the query
					completeQuery := bindParameters(query, entry.Detail)
					log.Printf("Bound query: %s", completeQuery)

					// Determine error message
					var errMsg *string
					if entry.ErrorSeverity == "ERROR" || entry.ErrorSeverity == "FATAL" || entry.ErrorSeverity == "PANIC" {
						errMsg = &entry.Message
						log.Printf("Error message set: %s", *errMsg)
					}

					// Create WebSocket message
					wsMessage := WebSocketMessage{
						Timestamp:  entry.Timestamp,
						Userhost:   strings.ToLower(entry.RemoteHost), // Convert to lowercase
						Username:   entry.User,
						ActionName: entry.Ps,
						SQL:        completeQuery,
						ErrorMsg:   errMsg,
					}

					// Marshal message to JSON
					messageBytes, err := json.Marshal(wsMessage)
					if err != nil {
						log.Printf("Error marshaling WebSocket message: %v", err)
						continue
					}
					log.Printf("Prepared WebSocket message: %s", string(messageBytes))

					// Send message to clients with matching userhost
					userhost := strings.ToLower(entry.RemoteHost) // Convert to lowercase
					clientsMu.Lock()
					for _, client := range clients[userhost] {
						err := client.WriteMessage(websocket.TextMessage, messageBytes)
						if err != nil {
							log.Printf("Write error for userhost %s: %v", userhost, err)
							client.Close()
							// Remove client from the list
							newConns := []*websocket.Conn{}
							for _, c := range clients[userhost] {
								if c != client {
									newConns = append(newConns, c)
								}
							}
							clients[userhost] = newConns
							if len(clients[userhost]) == 0 {
								delete(clients, userhost)
							}
						} else {
							log.Printf("Sent message to client for userhost %s", userhost)
						}
					}
					clientsMu.Unlock()
				} else {
					log.Printf("Skipping query: not a SELECT/INSERT/UPDATE/DELETE/TRUNCATE: %s", query)
				}
			} else {
				log.Printf("Skipping log: ps=%s, detail=%s, query=%s", entry.Ps, entry.Detail, cleanQuery(entry.Message))
			}
		}

		log.Printf("Tailing stopped for file %s, checking for new file", currentFile)
		t.Cleanup()
	}
}

func main() {
	go tailLogFile()

	http.HandleFunc("/ws/", handleWebSocket)
	http.Handle("/", http.FileServer(http.Dir(".")))

	log.Println("Server started on :8090")
	log.Fatal(http.ListenAndServe(":8090", nil))
}
