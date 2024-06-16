package main

import "fmt"

func main() {
	number := 40

	// chay tuan tu
	// for i := 1; i <= number; i++ {
	// 	fmt.Println(fib(i))
	// }

	// chay nhieu goroutine
	// co the tinh toan cac gia tri doc lap voi nhau
	numberOfWoker := 20
	jobs := make(chan int, number)
	results := make(chan int, number)

	// kich hoat numberOfWoker
	for i := 0; i < numberOfWoker; i++ {
		go worker(jobs, results)
	}

	for i := 1; i <= number; i++ {
		jobs <- i
	}

	close(jobs)

	// doc gia tri tu day sofibo da duoc tinh => duyet qua channel results de in ket qua
	for j := 0; j < number; j++ {
		fmt.Println(<-results)
	}
}

// jobs : channel chi doc
// results : channel chi ghi
func worker(jobs <-chan int, results chan<- int) {
	for n := range jobs {
		results <- fib(n)
	}
}
func fib(n int) int {
	// de quy
	if n <= 1 {
		return n
	}
	return fib(n-1) + fib(n-2)
}
