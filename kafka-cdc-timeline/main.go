package main

import (
	"context"
	"encoding/json"
	"fmt"
	"httt-dev/kafka-cdc-server/config"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/gorilla/websocket"
)

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
	wsClients = struct {
		sync.RWMutex
		clients map[*websocket.Conn]string
	}{
		clients: make(map[*websocket.Conn]string),
	}
)

type CDCMessage struct {
	Op     string                 `json:"op"`
	Before map[string]interface{} `json:"before"`
	After  map[string]interface{} `json:"after"`
}

func convertMicrosToDateTime(micros int64) string {
	if micros == 0 {
		return ""
	}
	millis := micros / 1000
	remainingMicros := micros % 1000
	t := time.Unix(0, millis*int64(time.Millisecond))
	return fmt.Sprintf("%s.%03d", t.Format("2006-01-02 15:04:05.000"), remainingMicros)
}

func isMicrosDatetime(value interface{}, key string) bool {
	if num, ok := value.(float64); ok {
		minMicros := float64(0)
		maxMicros := float64(2147483647000000)
		isDateKey := regexp.MustCompile(`(?i)DATETIME|TIMESTAMP|DATE|ALLOCATED_YMD|POST_TERM_TO|POST_TERM_FROM`).MatchString(key)
		return num >= minMicros && num <= maxMicros && isDateKey
	}
	return false
}

func convertDatetimeFields(obj map[string]interface{}) map[string]interface{} {
	if obj == nil {
		return nil
	}
	result := make(map[string]interface{})
	for k, v := range obj {
		if isMicrosDatetime(v, k) {
			result[k] = convertMicrosToDateTime(int64(v.(float64)))
		} else {
			result[k] = v
		}
	}
	return result
}

func findOperationUserId(obj map[string]interface{}) string {
	if obj == nil {
		return ""
	}
	targetFields := []string{"update_user_id", "delete_user_id"}
	for _, field := range targetFields {
		if value, exists := obj[field]; exists {
			if strValue, ok := value.(string); ok && strValue != "" {
				return strings.ToLower(strValue)
			}
		}
	}
	return ""
}

func checkAndRecoveryConnectorStatus(connectorName, configFilePath string) error {
	client := &http.Client{Timeout: 10 * time.Second}
	url := fmt.Sprintf("%s/connectors/%s/status", config.AppConfig.KafkaConnect.ConnectorHost, connectorName)

	// Check status
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// Delete connector
		deleteURL := fmt.Sprintf("%s/connectors/%s", config.AppConfig.KafkaConnect.ConnectorHost, connectorName)
		req, _ := http.NewRequest("DELETE", deleteURL, nil)
		client.Do(req)

		// Read config file
		configData, err := os.ReadFile(configFilePath)
		if err != nil {
			return err
		}

		// Create new connector
		createURL := fmt.Sprintf("%s/connectors", config.AppConfig.KafkaConnect.ConnectorHost)
		req, _ = http.NewRequest("POST", createURL, strings.NewReader(string(configData)))
		req.Header.Set("Content-Type", "application/json")
		resp, err = client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
	}

	return nil
}

func runConnectorHealthCheck() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		go checkAndRecoveryConnectorStatus("oracle-connector", "./connectors/oracle-connector.json")
		go checkAndRecoveryConnectorStatus("postgres-connector", "./connectors/postgres-connector.json")
	}
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	userId := strings.ToLower(strings.TrimPrefix(r.URL.Path, "/"))
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade error: %v", err)
		return
	}

	wsClients.Lock()
	wsClients.clients[conn] = userId
	wsClients.Unlock()

	defer func() {
		wsClients.Lock()
		delete(wsClients.clients, conn)
		wsClients.Unlock()
		conn.Close()
	}()

	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			break
		}
	}
}

func broadcastMessage(msg CDCMessage) {
	messageUpdateUserId := findOperationUserId(msg.After)
	if messageUpdateUserId == "" {
		messageUpdateUserId = findOperationUserId(msg.Before)
	}

	wsClients.RLock()
	defer wsClients.RUnlock()

	for client, userId := range wsClients.clients {
		if messageUpdateUserId == "" || messageUpdateUserId == userId {
			if err := client.WriteJSON(msg); err != nil {
				log.Printf("Error sending message to client: %v", err)
			}
		}
	}
}

func runConsumer(ctx context.Context, consumer sarama.Consumer, topic string, sourceType string) {
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Printf("Failed to start consumer for %s: %v", sourceType, err)
		return
	}
	defer partitionConsumer.Close()

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			var cdcMsg CDCMessage
			if err := json.Unmarshal(msg.Value, &cdcMsg); err != nil {
				log.Printf("Failed to parse message from %s: %v", sourceType, err)
				continue
			}

			if cdcMsg.Op == "c" || cdcMsg.Op == "u" {
				cdcMsg.After = convertDatetimeFields(cdcMsg.After)
				cdcMsg.Before = convertDatetimeFields(cdcMsg.Before)
			} else if cdcMsg.Op == "d" {
				cdcMsg.Before = convertDatetimeFields(cdcMsg.Before)
			}

			broadcastMessage(cdcMsg)

		case <-ctx.Done():
			return
		}
	}
}

func main() {
	if err := config.LoadConfig(); err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Start connector health check
	go runConnectorHealthCheck()

	// Setup Kafka consumer
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Consumer.Return.Errors = true

	oracleConsumer, err := sarama.NewConsumer(config.AppConfig.Kafka.Brokers, kafkaConfig)
	if err != nil {
		log.Fatalf("Failed to create Oracle consumer: %v", err)
	}
	defer oracleConsumer.Close()

	postgresConsumer, err := sarama.NewConsumer(config.AppConfig.Kafka.Brokers, kafkaConfig)
	if err != nil {
		log.Fatalf("Failed to create PostgreSQL consumer: %v", err)
	}
	defer postgresConsumer.Close()

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start consumers in separate goroutines
	go runConsumer(ctx, oracleConsumer, config.AppConfig.Topics.Oracle, "Oracle")
	go runConsumer(ctx, postgresConsumer, config.AppConfig.Topics.Postgres, "PostgreSQL")

	// Setup HTTP server
	http.HandleFunc("/ws", handleWebSocket)
	http.Handle("/", http.FileServer(http.Dir("public")))

	// Start HTTP server
	log.Printf("Server running at http://localhost:%d", config.AppConfig.Server.Port)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", config.AppConfig.Server.Port), nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
