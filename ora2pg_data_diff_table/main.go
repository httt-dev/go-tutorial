package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"ora2pg_data_diff_table/comparison"
	"ora2pg_data_diff_table/config"
	"ora2pg_data_diff_table/database"
	"ora2pg_data_diff_table/output"
)

func main() {
	// Setup logging
	logFile, err := os.OpenFile("comparison.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalf("Error opening log file: %v", err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	// Load configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Error loading configuration: %v", err)
	}
	log.Printf("Configuration loaded: %+v", cfg)

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
	defer cancel()

	// Connect to databases
	oracleDB, err := database.NewOracleDB(cfg.OracleConnStr)
	if err != nil {
		log.Fatalf("Error connecting to Oracle: %v", err)
	}
	defer oracleDB.Close()
	log.Println("Connected to Oracle database")

	postgresDB, err := database.NewPostgresDB(ctx, cfg.PostgresConnStr)
	if err != nil {
		log.Fatalf("Error connecting to PostgreSQL: %v", err)
	}
	defer postgresDB.Close()
	log.Println("Connected to PostgreSQL database")

	// Create comparer
	comparerConfig := &comparison.Config{
		TableOracle:    cfg.TableOracle,
		TablePostgres:  cfg.TablePostgres,
		WhereClause:    cfg.WhereClause,
		KeyColumns:     cfg.KeyColumns,
		CompareColumns: cfg.CompareColumns,
		IgnoreColumns:  cfg.IgnoreColumns,
	}
	log.Printf("Comparer config: %+v", comparerConfig)

	comparer := comparison.NewComparer(oracleDB, postgresDB, comparerConfig, cfg.WorkerCount, cfg.BatchSize)

	// Start comparison
	startTime := time.Now()
	log.Println("Starting comparison...")
	result, err := comparer.Compare(ctx)
	if err != nil {
		log.Fatalf("Error comparing tables: %v", err)
	}
	endTime := time.Now()

	// Generate report
	outputPath := fmt.Sprintf("comparison_report_%s.html", time.Now().Format("20060102_150405"))
	if err := output.GenerateReport(result, cfg); err != nil {
		log.Fatalf("Error generating report: %v", err)
	}

	// Print summary to both console and log
	summary := fmt.Sprintf("\nComparison completed in %s\nTotal records: %d\nDifferent records: %d\n",
		endTime.Sub(startTime), result.TotalRecords, result.DifferentRecords)
	if result.IsIdentical {
		summary += "Tables are identical!"
	} else {
		summary += fmt.Sprintf("Tables are different. See report at: %s", outputPath)
	}
	fmt.Print(summary)
	log.Print(summary)
}
