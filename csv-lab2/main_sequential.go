// package main1

// import (
// 	"encoding/csv"
// 	"errors"
// 	"fmt"
// 	"os"
// 	"time"

// 	"github.com/brianvoe/gofakeit"
// )

// const (
// 	numRowsTotal   = 1000
// 	numGoroutines  = 1
// 	numRowsPerFile = numRowsTotal / numGoroutines
// )

// func main1() {
// 	GenerateLargeCSV(numRowsTotal, "test_data")
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
