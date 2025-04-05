package main

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log"
	"math/big"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	_ "github.com/lib/pq"
)

type Record struct {
	Before map[string]interface{} `json:"before"`
	After  map[string]interface{} `json:"after"`
	Op     string                 `json:"op"`
}

type KafkaMessage struct {
	Schema  interface{} `json:"schema"`
	Payload Record      `json:"payload"`
}

// Struct biểu diễn VariableScaleDecimal từ Debezium
type VariableScaleDecimal struct {
	Scale int    `json:"scale"`
	Value string `json:"value"` // Chuỗi Base64
}

func decodeVariableScaleDecimal(vsd VariableScaleDecimal) (int64, error) {
	// Giải mã chuỗi Base64
	bytes, err := base64.StdEncoding.DecodeString(vsd.Value)
	if err != nil {
		return 0, err
	}

	// Chuyển bytes thành số nguyên lớn (big.Int)
	bigInt := new(big.Int).SetBytes(bytes)

	// Với scale = 0, chỉ cần lấy giá trị nguyên
	return bigInt.Int64(), nil
}

func getID(data map[string]interface{}) (int64, error) {
	idRaw, ok := data["ID"]
	if !ok {
		log.Printf("ID not found in data: %v", data)
		return 0, errors.New("ID not found in data")
	}

	// Chuyển đổi thành VariableScaleDecimal
	idBytes, err := json.Marshal(idRaw)
	if err != nil {
		return 0, err
	}

	var vsd VariableScaleDecimal
	if err := json.Unmarshal(idBytes, &vsd); err != nil {
		return 0, err
	}

	return decodeVariableScaleDecimal(vsd)
}

func main() {
	// Cấu hình Kafka Consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:19092",
		"group.id":          "postgres-sync-group",
		"auto.offset.reset": "earliest",
		// "debug":             "all",
	})
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}
	defer consumer.Close()

	consumer.SubscribeTopics([]string{"oracle.TEST_USER.TEST_TABLE"}, nil)

	// Kết nối PostgreSQL
	connStr := "host=localhost port=5432 user=postgres password=Abc12345 dbname=bo_test sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer db.Close()

	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			log.Printf("Error reading message: %v", err)
			continue
		}

		// Parse dữ liệu từ Kafka vào struct trung gian
		var kafkaMsg KafkaMessage
		err = json.Unmarshal(msg.Value, &kafkaMsg)
		if err != nil {
			log.Printf("Error parsing JSON: %v", err)
			continue
		}

		// Lấy phần payload (chứa before, after, op)
		record := kafkaMsg.Payload

		switch record.Op {
		case "c":
			insertData(db, record.After)
		case "u":
			updateData(db, record.After)
		case "d":
			deleteData(db, record.Before)
		default:
			log.Printf("Unknown operation: %s", record.Op)
		}
	}
}

func insertData(db *sql.DB, data map[string]interface{}) {
	log.Printf("insert data : %v", data)
	id, err := getID(data)
	if err != nil {
		log.Printf("Error decoding ID: %v", err)
		return
	}
	name, _ := data["NAME"].(string)
	log.Printf("Inserting: ID=%d, NAME=%s", id, name)

	// _, err := db.Exec("INSERT INTO test_table (id, name) VALUES ($1, $2)", data["ID"], data["NAME"])
	// if err != nil {
	// 	log.Printf("Error inserting data: %v", err)
	// }
}

func updateData(db *sql.DB, data map[string]interface{}) {

	log.Printf("update data : %v", data)
	id, err := getID(data)
	if err != nil {
		log.Printf("Error decoding ID: %v", err)
		return
	}
	name, _ := data["NAME"].(string)
	log.Printf("Inserting: ID=%d, NAME=%s", id, name)

	// _, err := db.Exec("UPDATE test_table SET name = $1 WHERE id = $2", data["NAME"], data["ID"])
	// if err != nil {
	// 	log.Printf("Error updating data: %v", err)
	// }
}

func deleteData(db *sql.DB, data map[string]interface{}) {
	log.Printf("delete data : %v", data)
	id, err := getID(data)
	if err != nil {
		log.Printf("Error decoding ID: %v", err)
		return
	}
	name, _ := data["NAME"].(string)
	log.Printf("Inserting: ID=%d, NAME=%s", id, name)

	// _, err := db.Exec("DELETE FROM test_table WHERE id = $1", data["ID"])
	// if err != nil {
	// 	log.Printf("Error deleting data: %v", err)
	// }
}
