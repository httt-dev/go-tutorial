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
var MAX_TASKS = 10
var MAX_ROWS = 100
var TOTAL_ROWS = 10000
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
					if rowIndex == from {
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

	go func() {
		for books := range dataChannel {
			for _, book := range books {
				// record := []string{book.ID, book.Name, book.JAN, book.Publisher}
				fmt.Println("Read data from channel", book.ID)
			}
		}

	}()

	for i := 0; i < MAX_TASKS; i++ {

		go func(startLine int, execOrder int) {
			defer func() {
				endTaskChan <- true
			}()

			if startLine > TOTAL_ROWS {
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

	time.Sleep(time.Second * 100)
}
