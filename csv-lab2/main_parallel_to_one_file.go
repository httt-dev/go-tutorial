package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/brianvoe/gofakeit"
)

const (
	numRowsTotal   = 10_000_000
	numGoroutines  = 1
	numRowsPerFile = numRowsTotal / numGoroutines
)

func main() {
	GenerateLargeCSVParallelToOneFile(numRowsPerFile, numGoroutines, "test_data")
}

func generateFakeRow() []string {
	// 1 year ago
	startDate := time.Now().AddDate(-1, 0, 0)
	endDate := time.Now()
	return []string{
		gofakeit.UUID(),
		fmt.Sprintf("%d", gofakeit.Uint8()),
		gofakeit.DateRange(startDate, endDate).Format(time.DateTime),
		gofakeit.DateRange(startDate, endDate).Format(time.DateTime),
	}
}

// Parallelize CSV generation while writing the same file
func GenerateLargeCSVParallelToOneFile(numRows, numGoroutines int, fileName string) {
	err := os.Mkdir("data", 0777)
	if err != nil {
		if !errors.Is(err, os.ErrExist) {
			panic(err)
		}
	}
	file, err := os.Create(fmt.Sprintf("data/%s.csv", fileName))
	if err != nil {
		if !errors.Is(err, os.ErrExist) {
			panic(err)
		}
	}
	defer file.Close()

	var wg sync.WaitGroup
	// Add numGoroutines to the WaitGroup
	wg.Add(numGoroutines)
	fmt.Println("numGoroutines = ", numGoroutines)

	writer, err := NewCSVWriter(file)
	if err != nil {
		panic(err)
	}
	for i := 0; i < numGoroutines; i++ {
		// Call GenerateLargeCSV in a goroutine for numGoroutines times
		go func(wg *sync.WaitGroup, i int, writer *CsvWriter) {
			fmt.Println("numRows = ", numRows)
			GenerateLargeCSVWithLock(numRows, writer)
			// Decrement the WaitGroup counter after each goroutine finishes
			defer wg.Done()
		}(&wg, i, writer)
	}
	// Wait for all goroutines to finish
	wg.Wait()
	fmt.Printf("Done GenerateLargeCSVParallelToOneFile")
}

func GenerateLargeCSVWithLock(numRows int, writer *CsvWriter) {
	for i := 0; i < numRows; i++ {
		row := generateFakeRow()
		if err := writer.Write(row); err != nil {
			panic(err)
		}
	}
	writer.Flush()
}

// thread safe csv writer
type CsvWriter struct {
	mutex     *sync.Mutex
	csvWriter *csv.Writer
}

func NewCSVWriter(file io.Writer) (*CsvWriter, error) {
	w := csv.NewWriter(file)
	return &CsvWriter{csvWriter: w, mutex: &sync.Mutex{}}, nil
}

// lock and write
func (w *CsvWriter) Write(row []string) error {
	w.mutex.Lock()
	err := w.csvWriter.Write(row)
	if err != nil {
		return err
	}
	w.mutex.Unlock()
	return nil
}

// lock and flush
func (w *CsvWriter) Flush() {
	w.mutex.Lock()
	w.csvWriter.Flush()
	w.mutex.Unlock()
}
