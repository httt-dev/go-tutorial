// package large_csv_generator

// import (
// 	"log"
// 	"os"
// 	"testing"
// )

// const (
// 	totalNumRows = 100000000
// 	fileName     = "test_data"
// )

// func BenchmarkGenerateLargeCSVParallel(b *testing.B) {
// 	GenerateLargeCSVParallel(totalNumRows/10, 10, fileName)
// 	defer CleanUp()

// }

// func BenchmarkGenerateLargeCSV(b *testing.B) {
// 	GenerateLargeCSV(totalNumRows, fileName)
// 	defer CleanUp()
// }

// func BenchmarkGenerateLargeCSVParallelToOneFile(b *testing.B) {
// 	GenerateLargeCSVParallelToOneFile(totalNumRows/10, 10, fileName)
// 	defer CleanUp()
// }

// func CleanUp() {
// 	err := os.RemoveAll("data")
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// }
