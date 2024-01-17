package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
)

const apiEndpoint = "http://localhost:8080/api/books?page="

type Book struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	JAN       string `json:"jan"`
	Publisher string `json:"publisher"`
}

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

func writeDataToFile(filename string, dataChannel chan []Book, done chan bool) {
	file, err := os.Create(filename)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for books := range dataChannel {
		for _, book := range books {
			record := []string{book.ID, book.Name, book.JAN, book.Publisher}
			err := writer.Write(record)
			if err != nil {
				fmt.Println("Error writing to CSV file:", err)
				break
			}
		}
	}

	done <- true
}

func main() {
	var wg sync.WaitGroup
	dataChannel := make(chan []Book, 10) // Adjust the buffer size as needed
	done := make(chan bool)
	pages := make(chan int)

	// Start a goroutine to write data to the CSV file
	go writeDataToFile("output.csv", dataChannel, done)

	// Start a goroutine to fetch data for each page
	go func() {
		for page := range pages {
			wg.Add(1)
			go fetchData(page, &wg, dataChannel)
		}
	}()

	// Start fetching pages starting from 1
	page := 1
	for {
		pages <- page
		page++
		// You can add a sleep here to avoid hitting the API too fast
		// time.Sleep(time.Millisecond * 500)

		// Check for the completion of fetching data from all pages
		if page > 10 { // Adjust the condition based on the total number of pages
			close(pages)
			break
		}
	}

	// Wait for all fetch goroutines to complete
	go func() {
		wg.Wait()
		close(dataChannel)
	}()

	// Wait for the write goroutine to complete
	<-done
}
