package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"ora2postgres_p1/config"
	"ora2postgres_p1/internal/database"
	"ora2postgres_p1/internal/migration"
	"ora2postgres_p1/internal/models"
)

var summaryList []models.TableSummary

func main() {
	// Set up logging
	setupLogging()

	startTime := time.Now()
	defer func() {
		// Print summary
		printSummary()

		log.Printf("Total execution time: %v", time.Since(startTime))
	}()

	// Load configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Error loading configuration: %v", err)
	}

	// Parse table mappings
	tableMappings := parseTableMappings(cfg.TableNames)
	tbl, err := json.MarshalIndent(tableMappings, "", "  ")
	if err != nil {
		log.Fatalf("Error parsing table mapping: %v", err)
	}
	log.Printf("List of tables to copy:\n%v\n", string(tbl))

	// Connect to Source Oracle
	srcOracleDB, err := database.NewOracleDB(cfg.SrcOracleDSN)
	if err != nil {
		log.Fatalf("Error creating SOURCE Oracle pool: %v", err)
	}
	defer srcOracleDB.Close()

	// Connect to Destination Postgres
	dstPostgresDB, err := database.NewPostgresDB(cfg.DstPostgresDSN)
	if err != nil {
		log.Fatalf("Error creating PostgreSQL pool: %v", err)
	}
	defer dstPostgresDB.Close()

	// Set session_replication_role to replica
	if err := dstPostgresDB.SetSessionRepliRole("replica"); err != nil {
		log.Fatalf("Error setting session_replication_role to replica: %v", err)
	}

	// Optimize PostgreSQL session parameters for bulk insert
	if err := dstPostgresDB.OptimizeSessionForBulkInsert(); err != nil {
		log.Printf("Warning: Error optimizing PostgreSQL session parameters: %v", err)
	}

	defer func() {
		// Reset PostgreSQL session parameters to safe values
		if err := dstPostgresDB.ResetSessionParameters(); err != nil {
			log.Printf("Warning: Error resetting PostgreSQL session parameters: %v", err)
		}

		// Set session_replication_role back to origin
		if err := dstPostgresDB.SetSessionRepliRole("origin"); err != nil {
			log.Printf("Error setting session_replication_role back to origin: %v", err)
		}
	}()

	for _, mapping := range tableMappings {
		srcTable := extractTableName(mapping.SrcTable)
		dstTable := strings.ToLower(extractTableName(mapping.DstTable))

		log.Printf(strings.Repeat("-", 20)+"Starting to process table: source=%s, destination=%s"+strings.Repeat("-", 20), srcTable, dstTable)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Disable logging before starting
		err := dstPostgresDB.DisableLogging(dstTable)
		if err != nil {
			log.Printf("\033[31m[ERROR] Error disabling logging for table %s: %v\033[0m", dstTable, err)
			continue
		}

		defer func(dstTable string) {
			if err := dstPostgresDB.EnableLogging(dstTable); err != nil {
				log.Printf("\033[31m[ERROR] Error enabling logging for table %s: %v\033[0m", dstTable, err)
			}
		}(dstTable)

		// Create migration config
		migrationConfig := &models.MigrationConfig{
			ChanQueue:      models.ChanQueue,
			BatchSize:      models.BatchSize,
			LogReadedRows:  models.LogReadedRows,
			PartitionBy:    cfg.PartitionBy,
			PartitionCount: cfg.PartitionCount,
			FilterWhere:    cfg.FilterWhere,
			ExpectedDBName: cfg.ExpectedDBName,
		}

		// Perform the data migration process for the current table
		err = migration.MigrateTable(ctx, cancel, srcOracleDB, dstPostgresDB, srcTable, dstTable, migrationConfig, &summaryList)
		if err != nil {
			log.Printf("\033[31m[ERROR] Error processing table source=%s, destination=%s: %v\033[0m", srcTable, dstTable, err)
			continue
		}
		log.Printf("Finished processing table: source=%s, destination=%s", srcTable, dstTable)
	}

	log.Printf("Data migration completed.")
}

// setupLogging sets up logging to both console and file
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

// parseTableMappings parses the table names from the environment variable TABLE_NAME
func parseTableMappings(tableNames string) []models.TableMapping {
	var mappings []models.TableMapping
	tables := strings.Split(tableNames, ",")
	for _, table := range tables {
		table = strings.TrimSpace(table)
		if table == "" {
			continue
		}
		parts := strings.Split(table, ":")
		srcTable := strings.TrimSpace(parts[0])
		srcTable = strings.ToUpper(srcTable)  // Convert to uppercase for consistency
		dstTable := strings.ToUpper(srcTable) // Default to source table name
		if len(parts) > 1 {
			dstTable = strings.TrimSpace(parts[1])
		}
		mappings = append(mappings, models.TableMapping{SrcTable: srcTable, DstTable: dstTable})
	}
	return mappings
}

// extractTableName extracts the table name from the input string
func extractTableName(table string) string {
	parts := strings.Split(table, " ")
	if len(parts) > 0 {
		return strings.TrimSpace(parts[0])
	}
	return ""
}

// printSummary prints a summary of the data migration process
func printSummary() {
	log.Println("=== SUMMARY ===")
	log.Printf("%-40s %-15s %-40s %-15s %-10s\n", "Source Table", "Source Rows", "Destination Table", "Copied Rows", "Status")
	log.Println(strings.Repeat("-", 120))
	for _, summary := range summaryList {
		log.Printf("%-40s %-15d %-40s %-15d %-10s\n",
			summary.SourceTable, summary.SourceRowCount, summary.DestinationTable, summary.CopiedRowCount, summary.Status)
	}
	log.Println(strings.Repeat("-", 120))
} 