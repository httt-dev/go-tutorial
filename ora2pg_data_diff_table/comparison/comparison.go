package comparison

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"math"
	"strings"
	"sync"
	"time"

	"ora2pg_data_diff_table/database"
)

// ComparisonResult represents the result of comparing two tables
type ComparisonResult struct {
	IsIdentical      bool
	Differences      []database.Record
	TotalRecords     int
	DifferentRecords int
}

// Comparer handles the comparison logic
type Comparer struct {
	oracleDB    database.Database
	postgresDB  database.Database
	config      *Config
	workerCount int
	batchSize   int
}

// Config holds comparison configuration
type Config struct {
	TableOracle    string
	TablePostgres  string
	WhereClause    string
	KeyColumns     []string
	CompareColumns []string
	IgnoreColumns  []string
}

// NewComparer creates a new Comparer instance
func NewComparer(oracleDB, postgresDB database.Database, config *Config, workerCount, batchSize int) *Comparer {
	return &Comparer{
		oracleDB:    oracleDB,
		postgresDB:  postgresDB,
		config:      config,
		workerCount: workerCount,
		batchSize:   batchSize,
	}
}

// Compare performs the comparison between Oracle and PostgreSQL tables
func (c *Comparer) Compare(ctx context.Context) (*ComparisonResult, error) {
	// Get total record counts
	oracleCount, err := c.oracleDB.CountRecords(ctx, c.config.TableOracle, c.config.WhereClause)
	if err != nil {
		return nil, fmt.Errorf("error counting Oracle records: %v", err)
	}

	pgCount, err := c.postgresDB.CountRecords(ctx, c.config.TablePostgres, c.config.WhereClause)
	if err != nil {
		return nil, fmt.Errorf("error counting PostgreSQL records: %v", err)
	}

	log.Printf("Oracle count: %d, PostgreSQL count: %d", oracleCount, pgCount)

	// Get columns to compare
	oracleColumns, err := c.oracleDB.GetColumns(ctx, c.config.TableOracle)
	if err != nil {
		return nil, fmt.Errorf("error getting Oracle columns: %v", err)
	}
	log.Printf("Oracle columns: %v", oracleColumns)

	pgColumns, err := c.postgresDB.GetColumns(ctx, c.config.TablePostgres)
	if err != nil {
		return nil, fmt.Errorf("error getting PostgreSQL columns: %v", err)
	}
	log.Printf("PostgreSQL columns: %v", pgColumns)

	// Filter columns based on configuration
	filteredColumns := filterColumns(oracleColumns, pgColumns, c.config.CompareColumns, c.config.IgnoreColumns)
	log.Printf("Filtered columns: %v", filteredColumns)

	// Ensure key columns are included
	columnsToCompare := ensureKeyColumnsIncluded(filteredColumns, c.config.KeyColumns)
	log.Printf("Columns to compare: %v", columnsToCompare)

	// Create channels for job distribution and result collection
	jobs := make(chan int, c.workerCount*2)
	results := make(chan []database.Record, c.workerCount*2)
	errors := make(chan error, c.workerCount*2)

	// Create a WaitGroup to track worker completion
	var wg sync.WaitGroup

	// Start workers
	log.Printf("Starting %d workers", c.workerCount)
	for i := 0; i < c.workerCount; i++ {
		wg.Add(1)
		workerID := i // Create a new variable for each iteration
		go func() {
			defer wg.Done()
			log.Printf("Worker %d started", workerID)
			c.worker(ctx, &wg, jobs, results, errors, columnsToCompare)
			log.Printf("Worker %d finished", workerID)
		}()
	}

	// Start a goroutine to send jobs
	go func() {
		defer close(jobs)
		for offset := 0; offset < oracleCount; offset += c.batchSize {
			select {
			case jobs <- offset:
				log.Printf("Sending job for offset %d", offset)
			case <-ctx.Done():
				return
			}
		}
	}()

	// Start a goroutine to collect results
	var allDifferences []database.Record
	go func() {
		for {
			select {
			case differences, ok := <-results:
				if !ok {
					return
				}
				if len(differences) > 0 {
					log.Printf("Received %d differences from worker", len(differences))
					allDifferences = append(allDifferences, differences...)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for all workers to complete
	wg.Wait()
	close(results)

	// Check for any errors
	select {
	case err := <-errors:
		return nil, fmt.Errorf("error during comparison: %v", err)
	default:
	}

	log.Printf("Found %d differences", len(allDifferences))
	result := &ComparisonResult{
		IsIdentical:      len(allDifferences) == 0,
		Differences:      allDifferences,
		TotalRecords:     oracleCount,
		DifferentRecords: len(allDifferences),
	}
	return result, nil
}

func (c *Comparer) worker(ctx context.Context, wg *sync.WaitGroup, jobs <-chan int, results chan<- []database.Record, errors chan<- error, columns []string) {
	// Create a pool of goroutines for fetching data
	fetchPool := make(chan struct{}, 4) // Limit concurrent fetches

	for offset := range jobs {
		batchStartTime := time.Now()
		limit := c.batchSize
		if offset+limit > c.batchSize*2 {
			limit = c.batchSize
		}

		log.Printf("Worker processing batch at offset %d with limit %d", offset, limit)

		// Fetch records from both databases in parallel with rate limiting
		var oracleRecords, pgRecords []database.Record
		var oracleErr, pgErr error
		var wgFetch sync.WaitGroup

		wgFetch.Add(2)

		// Fetch Oracle data
		oracleStartTime := time.Now()
		fetchPool <- struct{}{} // Acquire semaphore
		go func() {
			defer wgFetch.Done()
			defer func() { <-fetchPool }() // Release semaphore
			oracleRecords, oracleErr = c.oracleDB.FetchRecords(ctx, c.config.TableOracle, columns, c.config.KeyColumns, c.config.WhereClause, offset, limit)
			if oracleErr != nil {
				errors <- fmt.Errorf("error fetching Oracle records at offset %d: %v", offset, oracleErr)
			}
			log.Printf("Oracle fetch completed in %v for offset %d", time.Since(oracleStartTime), offset)
		}()

		// Fetch PostgreSQL data
		pgStartTime := time.Now()
		fetchPool <- struct{}{} // Acquire semaphore
		go func() {
			defer wgFetch.Done()
			defer func() { <-fetchPool }() // Release semaphore
			pgRecords, pgErr = c.postgresDB.FetchRecords(ctx, c.config.TablePostgres, columns, c.config.KeyColumns, c.config.WhereClause, offset, limit)
			if pgErr != nil {
				errors <- fmt.Errorf("error fetching PostgreSQL records at offset %d: %v", offset, pgErr)
			}
			log.Printf("PostgreSQL fetch completed in %v for offset %d", time.Since(pgStartTime), offset)
		}()

		wgFetch.Wait()
		log.Printf("Total fetch time for offset %d: %v", offset, time.Since(batchStartTime))

		if oracleErr != nil || pgErr != nil {
			return
		}

		log.Printf("Fetched %d records from Oracle and %d from PostgreSQL", len(oracleRecords), len(pgRecords))

		// Create map of PostgreSQL records by key for faster lookup
		mapStartTime := time.Now()
		pgMap := make(map[string]database.Record)
		pgHashMap := make(map[string]string)

		// Process PostgreSQL records
		for _, pg := range pgRecords {
			key := makeKey(pg.KeyValues, c.config.KeyColumns)
			pgMap[key] = pg
			pgHashMap[key] = calculateRecordHash(pg.ColValues, columns)
		}
		log.Printf("Map creation completed in %v for offset %d", time.Since(mapStartTime), offset)

		// Compare records
		compareStartTime := time.Now()
		var differences []database.Record

		// Process Oracle records in parallel
		var wgCompare sync.WaitGroup
		diffChan := make(chan database.Record, len(oracleRecords))
		recordChan := make(chan database.Record, len(oracleRecords))

		// Start comparison workers
		numCompareWorkers := 4
		for i := 0; i < numCompareWorkers; i++ {
			wgCompare.Add(1)
			go func() {
				defer wgCompare.Done()
				for oracle := range recordChan {
					key := makeKey(oracle.KeyValues, c.config.KeyColumns)
					pg, exists := pgMap[key]

					if !exists {
						oracle.IsDifferent = true
						oracle.DiffFields = append(oracle.DiffFields, "Record exists in Oracle but not in PostgreSQL")
						diffChan <- oracle
						log.Printf("Record not found in PostgreSQL: key=%v", key)
						continue
					}

					// Compare using hash first
					oracleHash := calculateRecordHash(oracle.ColValues, columns)
					pgHash := pgHashMap[key]

					if oracleHash != pgHash {
						// If hashes are different, compare individual columns
						var diffFields []string
						for _, col := range columns {
							oracleVal := oracle.ColValues[strings.ToLower(col)]
							pgVal := pg.ColValues[strings.ToLower(col)]

							if !compareValuesEqual(oracleVal, pgVal) {
								diffFields = append(diffFields, col)
								log.Printf("Difference found in column %s for key %v: Oracle='%v' (%T), PostgreSQL='%v' (%T)",
									col, oracle.KeyValues, oracleVal, oracleVal, pgVal, pgVal)
							}
						}

						if len(diffFields) > 0 {
							oracle.IsDifferent = true
							oracle.DiffFields = diffFields
							diffChan <- oracle
							log.Printf("Found differences in record with key %v: %v", key, diffFields)
						}
					}
				}
			}()
		}

		// Send records to comparison workers
		go func() {
			for _, oracle := range oracleRecords {
				recordChan <- oracle
			}
			close(recordChan)
		}()

		// Collect differences
		go func() {
			wgCompare.Wait()
			close(diffChan)
		}()

		// Collect results
		for diff := range diffChan {
			differences = append(differences, diff)
		}

		log.Printf("Comparison completed in %v for offset %d", time.Since(compareStartTime), offset)

		if len(differences) > 0 {
			select {
			case results <- differences:
				log.Printf("Sent %d differences to results channel", len(differences))
			case <-ctx.Done():
				errors <- ctx.Err()
				return
			}
		}

		log.Printf("Total batch processing time for offset %d: %v", offset, time.Since(batchStartTime))
	}
}

// calculateRecordHash calculates a hash for a record's values
func calculateRecordHash(values map[string]interface{}, columns []string) string {
	var valuesStr []string
	for _, col := range columns {
		val := values[strings.ToLower(col)]
		if val == nil {
			valuesStr = append(valuesStr, "NULL")
		} else {
			// Convert value to string and handle special cases
			switch v := val.(type) {
			case string:
				valuesStr = append(valuesStr, strings.TrimSpace(v))
			case time.Time:
				valuesStr = append(valuesStr, v.Format(time.RFC3339))
			default:
				valuesStr = append(valuesStr, fmt.Sprintf("%v", v))
			}
		}
	}

	// Join all values with a delimiter and calculate hash
	recordStr := strings.Join(valuesStr, "|")
	hash := sha256.Sum256([]byte(recordStr))
	return hex.EncodeToString(hash[:])
}

// compareValues compares values between two records
func compareValues(oracleValues, pgValues map[string]interface{}, compareCols []string) []string {
	var diffFields []string
	for _, col := range compareCols {
		oracleVal := oracleValues[col]
		pgVal := pgValues[col]

		if !compareValuesEqual(oracleVal, pgVal) {
			diffFields = append(diffFields, col)
		}
	}
	return diffFields
}

// compareValuesEqual compares two values for equality
func compareValuesEqual(val1, val2 interface{}) bool {
	// Handle nil values
	if val1 == nil && val2 == nil {
		return true
	}
	if val1 == nil || val2 == nil {
		log.Printf("One value is nil: val1=%v (%T), val2=%v (%T)", val1, val1, val2, val2)
		return false
	}

	// Handle string values
	if str1, ok1 := val1.(string); ok1 {
		if str2, ok2 := val2.(string); ok2 {
			// Trim whitespace and compare
			str1 = strings.TrimSpace(str1)
			str2 = strings.TrimSpace(str2)
			if str1 != str2 {
				log.Printf("String values differ: '%s' != '%s'", str1, str2)
				return false
			}
			return true
		}
	}

	// Handle time values
	if t1, ok1 := val1.(time.Time); ok1 {
		if t2, ok2 := val2.(time.Time); ok2 {
			if !t1.Equal(t2) {
				log.Printf("Time values differ: %v != %v", t1, t2)
				return false
			}
			return true
		}
	}

	// Handle numeric values
	switch v1 := val1.(type) {
	case int64:
		if v2, ok := val2.(int64); ok {
			if v1 != v2 {
				log.Printf("Int64 values differ: %d != %d", v1, v2)
				return false
			}
			return true
		}
	case float64:
		if v2, ok := val2.(float64); ok {
			// Use small epsilon for float comparison
			const epsilon = 1e-9
			if math.Abs(v1-v2) > epsilon {
				log.Printf("Float64 values differ: %f != %f", v1, v2)
				return false
			}
			return true
		}
	case int:
		if v2, ok := val2.(int); ok {
			if v1 != v2 {
				log.Printf("Int values differ: %d != %d", v1, v2)
				return false
			}
			return true
		}
	case float32:
		if v2, ok := val2.(float32); ok {
			// Use small epsilon for float comparison
			const epsilon = 1e-6
			if math.Abs(float64(v1-v2)) > epsilon {
				log.Printf("Float32 values differ: %f != %f", v1, v2)
				return false
			}
			return true
		}
	}

	// For other types, convert to string and compare
	str1 := fmt.Sprintf("%v", val1)
	str2 := fmt.Sprintf("%v", val2)
	if str1 != str2 {
		log.Printf("Values differ after string conversion: '%s' (%T) != '%s' (%T)",
			str1, val1, str2, val2)
		return false
	}
	return true
}

// makeKey creates a key string from key values
func makeKey(keyValues map[string]interface{}, keyCols []string) string {
	var keyParts []string
	for _, col := range keyCols {
		val := keyValues[strings.ToLower(col)]
		if val == nil {
			keyParts = append(keyParts, "NULL")
		} else {
			// Convert value to string and handle special cases
			switch v := val.(type) {
			case string:
				// Replace special characters with their hex representation
				keyParts = append(keyParts, fmt.Sprintf("%x", []byte(v)))
			case time.Time:
				keyParts = append(keyParts, v.Format(time.RFC3339))
			default:
				keyParts = append(keyParts, fmt.Sprintf("%v", v))
			}
		}
	}
	return strings.Join(keyParts, "|")
}

// filterColumns filters columns based on configuration
func filterColumns(oracleCols, pgCols, compareCols, ignoreCols []string) []string {
	// Convert all column names to lowercase for case-insensitive comparison
	oracleColsLower := make([]string, len(oracleCols))
	for i, col := range oracleCols {
		oracleColsLower[i] = strings.ToLower(col)
	}

	pgColsLower := make([]string, len(pgCols))
	for i, col := range pgCols {
		pgColsLower[i] = strings.ToLower(col)
	}

	// If compareCols is empty, use all common columns
	var columns []string
	if len(compareCols) == 0 {
		// Find common columns between Oracle and PostgreSQL
		commonCols := make(map[string]bool)
		for _, col := range oracleColsLower {
			commonCols[col] = true
		}
		for _, col := range pgColsLower {
			if commonCols[col] {
				columns = append(columns, col)
			}
		}
	} else {
		// Use specified columns
		compareColsLower := make([]string, len(compareCols))
		for i, col := range compareCols {
			compareColsLower[i] = strings.ToLower(col)
		}
		columns = compareColsLower
	}

	// Remove ignored columns
	if len(ignoreCols) > 0 {
		ignoreColsLower := make([]string, len(ignoreCols))
		for i, col := range ignoreCols {
			ignoreColsLower[i] = strings.ToLower(col)
		}
		var filteredCols []string
		for _, col := range columns {
			shouldIgnore := false
			for _, ignoreCol := range ignoreColsLower {
				if col == ignoreCol {
					shouldIgnore = true
					break
				}
			}
			if !shouldIgnore {
				filteredCols = append(filteredCols, col)
			}
		}
		columns = filteredCols
	}

	log.Printf("Filtered columns: %v", columns)
	return columns
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if strings.EqualFold(s, item) {
			return true
		}
	}
	return false
}

func ensureKeyColumnsIncluded(columns []string, keyColumns []string) []string {
	// Convert all to lowercase for case-insensitive comparison
	keyColumnsLower := make([]string, len(keyColumns))
	for i, col := range keyColumns {
		keyColumnsLower[i] = strings.ToLower(col)
	}

	// Create a map of existing columns for faster lookup
	existingColumns := make(map[string]bool)
	for _, col := range columns {
		existingColumns[strings.ToLower(col)] = true
	}

	// Add any missing key columns
	result := make([]string, len(columns))
	copy(result, columns)

	for _, keyCol := range keyColumnsLower {
		if !existingColumns[keyCol] {
			result = append(result, keyCol)
		}
	}

	return result
}
