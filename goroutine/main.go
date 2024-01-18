package main

import (
	"fmt"
	"sync"
	"time"
)

func init() {
	fmt.Println("initializing")
}

func main() {

	var wg = sync.WaitGroup{} // tao doi tuong rong

	wg.Add(2)
	// tao goroutines
	// go countMulti("sheep goroutines")
	// go countMulti("fish goroutines")
	go func() {
		countMulti("sheep")
		wg.Done() // hoan thanh goroutines : giam wg di 1 don vi
	}()
	go func() {
		countMulti("fish")
		wg.Done() // hoan thanh goroutines : giam wg di 1 don vi
	}()

	wg.Wait() // cho cho cac goroutines done , thay the cho sleep
	fmt.Println("Done")
	// count("sheep")
	// count("fish")

	// time.Sleep(time.Second * 2) // cho cho goroutine thuc thi xong <- cach nay khong dung

	testMutex()
}

func count(name string) {
	for i := 1; i <= 5; i++ {
		fmt.Println(name, i)
		time.Sleep(time.Second)
	}
}

func countMulti(name string) {
	for i := 1; i <= 5; i++ {
		fmt.Println(name, i)
		time.Sleep(time.Second)
	}
}

// mutex

var wg = sync.WaitGroup{}
var m = sync.RWMutex{}

var counter = 0

func testMutex() {

	for i := 0; i < 10; i++ {
		wg.Add(2)
		// moi lan lap tao 2 goroutines
		// tong cong co 20 goroutines
		m.RLock() // khoa lai de doc
		go sayHello()
		m.Lock() // khoa lai de ghi
		go increment()
	}
	wg.Wait()
}
func sayHello() {
	fmt.Printf("Hello #%v\n", counter)
	m.RUnlock() // mo khoa doc
	wg.Done()
}

func increment() {
	counter++
	m.Unlock() // mo khoa ghi
	wg.Done()
}
