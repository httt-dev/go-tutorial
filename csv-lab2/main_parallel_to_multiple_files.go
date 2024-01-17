// package main3

// import (
// 	"encoding/csv"
// 	"errors"
// 	"fmt"
// 	"os"
// 	"sync"
// 	"time"

// 	"github.com/brianvoe/gofakeit"
// )

// const (
// 	numRowsTotal   = 1000
// 	numGoroutines  = 1
// 	numRowsPerFile = numRowsTotal / numGoroutines
// )

// func main3() {
// 	GenerateLargeCSVParallel(numRowsPerFile, numGoroutines, "test_data")
// }

// // GenerateLargeCSV generates a CSV file with numRows rows
// func GenerateLargeCSV(numRows int, fileName string) {
// 	err := os.Mkdir("data", 0777)
// 	if err != nil {
// 		if !errors.Is(err, os.ErrExist) {
// 			panic(err)
// 		}
// 	}
// 	file, err := os.Create(fmt.Sprintf("data/%s.csv", fileName))
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer file.Close()

// 	writer := csv.NewWriter(file)
// 	for i := 0; i < numRows; i++ {
// 		row := generateFakeRow()
// 		if err := writer.Write(row); err != nil {
// 			panic(err)
// 		}
// 	}
// 	writer.Flush()
// }

// func generateFakeRow() []string {
// 	// 1 year ago
// 	startDate := time.Now().AddDate(-1, 0, 0)
// 	endDate := time.Now()
// 	return []string{
// 		gofakeit.UUID(),
// 		fmt.Sprintf("%d", gofakeit.Uint8()),
// 		gofakeit.DateRange(startDate, endDate).Format(time.DateTime),
// 		gofakeit.DateRange(startDate, endDate).Format(time.DateTime),
// 	}
// }

// // Parallelize CSV generation
// func GenerateLargeCSVParallel(numRows, numGoroutines int, fileName string) {
// 	var wg sync.WaitGroup
// 	// Add numGoroutines to the WaitGroup
// 	wg.Add(numGoroutines)

// 	for i := 0; i < numGoroutines; i++ {
// 		// Call GenerateLargeCSV in a goroutine for numGoroutines times
// 		go func(wg *sync.WaitGroup, i int) {
// 			fileName := fmt.Sprintf("%s_%d", fileName, i)
// 			GenerateLargeCSV(numRows, fileName)
// 			// Decrement the WaitGroup counter after each goroutine finishes
// 			defer wg.Done()
// 		}(&wg, i)
// 	}
// 	// Wait for all goroutines to finish
// 	wg.Wait()
// 	fmt.Printf("Done GenerateLargeCSVParallel")
// }
