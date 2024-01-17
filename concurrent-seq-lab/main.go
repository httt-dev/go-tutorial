package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

const apiEndpoint = "http://localhost:8080/api/books-index?startIndex="

type Book struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	JAN       string `json:"jan"`
	Publisher string `json:"publisher"`
}

var rowIndex = 0
var dataChannel chan []Book
var endTaskChan chan bool
var MAX_TASKS = 5
var MAX_ROWS = 10
var TOTAL_ROWS = 200
var m sync.Mutex

func fetchData(page int, wg *sync.WaitGroup, dataChannel chan []Book) {
	defer wg.Done()

	url := fmt.Sprintf("%s%d", apiEndpoint, page)
	response, err := http.Get(url)
	if err != nil {
		fmt.Println("Error fetching data from", url, ":", err)
		return
	}
	defer response.Body.Close()

	if response.StatusCode == http.StatusOK {
		var books []Book
		err := json.NewDecoder(response.Body).Decode(&books)
		if err != nil {
			fmt.Println("Error decoding JSON from", url, ":", err)
			return
		}
		dataChannel <- books
	}
}

func getResultBlock(from int, count int, dataChannel chan []Book) {

START:
	for {
		url := fmt.Sprintf("%s%d", apiEndpoint, from)
		response, err := http.Get(url)
		if err == nil {
			var books []Book
			if response.StatusCode == http.StatusOK {

				err := json.NewDecoder(response.Body).Decode(&books)
				if err != nil {
					fmt.Println("Error decoding JSON from", url, ":", err)
					return
				}

				for {
					m.Lock()
					row := rowIndex
					m.Unlock()
					if row == from {
						break
					}

				}

				dataChannel <- books

				m.Lock()
				rowIndex = rowIndex + len(books)
				m.Unlock()

			}

			if (len(books)) < count {
				from = from + len(books)
				count = count - len(books)

				goto START
			}

			return
		}
	}
}

func main() {

	dataChannel = make(chan []Book, 100000) // Adjust the buffer size as needed
	endTaskChan = make(chan bool, MAX_TASKS)
	done := make(chan bool)

	go func() {
		// defer func() {
		// 	fmt.Println("read finished")
		// 	done <- true
		// }()

		for books := range dataChannel {
			for _, book := range books {
				// record := []string{book.ID, book.Name, book.JAN, book.Publisher}
				fmt.Println("Read data from channel", book.ID)
				time.Sleep(20 * time.Millisecond) //Mi) //
			}

		}
		fmt.Println("read finished")
		done <- true
	}()

	for i := 0; i < MAX_TASKS; i++ {

		fmt.Println("task", i)

		go func(startLine int, execOrder int) {

			fmt.Println("startLine ", startLine)

			defer func() {
				fmt.Println("endTaskChan")
				endTaskChan <- true

			}()

			if startLine > TOTAL_ROWS {
				fmt.Println("startLine > TOTAL_ROWS")
				return
			}

			from := startLine

		GET_NEXT:
			count := MAX_ROWS
			if count+from > TOTAL_ROWS {
				count = TOTAL_ROWS - from
			}

			//fmt.Println("get result block of thread ", execOrder, " from ", from)

			getResultBlock(from, count, dataChannel)

			from = from + MAX_ROWS*MAX_TASKS

			if from < TOTAL_ROWS {
				goto GET_NEXT
			}

		}(i*MAX_ROWS, i)
	}

	// time.Sleep(time.Second * 100)

	// Chờ tất cả goroutine hoàn thành
	for i := 0; i < MAX_TASKS; i++ {
		fmt.Println("read endTaskChan")
		<-endTaskChan
	}

	// for i := range endTaskChan {
	// 	fmt.Println("read endTaskChan", i)
	// }

	close(endTaskChan)

	close(dataChannel)

	<-done

	fmt.Println("finished")
}
