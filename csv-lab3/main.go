package main

// Sample file for test: https://drive.google.com/file/d/1DFkJdX5UTnB_xL7g8xwkkdE8BxdurAhN/view?usp=sharing
import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"time"
)

type Sales struct {
	Region        string  `json:"region"`
	Country       string  `json:"country"`
	ItemType      string  `json:"item_type"`
	SaleChannel   string  `json:"sale_channel"`
	OrderPriority string  `json:"order_priority"`
	OrderDate     string  `json:"order_date"`
	OrderId       int64   `json:"order_id"`
	ShipDate      string  `json:"ship_date"`
	UnitSold      int64   `json:"unit_sold"`
	UnitPrice     float64 `json:"unit_price"`
	UnitCost      float64 `json:"unit_cost"`
	TotalRevenue  float64 `json:"total_revenue"`
	TotalCost     float64 `json:"total_cost"`
	TotalProfit   float64 `json:"total_profit"`
}

var mu sync.Mutex

var records = make(chan []string)

// https://medium.com/@snassr/processing-large-files-in-go-golang-6ea87effbfe2
// https://github.com/snassr/blog-0010-processinglargefilesingo
func main() {
	f, _ := os.Open("./1000k.csv")
	f1, _ := os.Open("./1000k.csv")
	defer f.Close()
	defer f1.Close()

	ts := time.Now()
	// basicRead(f)
	// basicRS(f)
	te := time.Now().Sub(ts)

	ts1 := time.Now()
	concuRead(f1)
	// concuRS(f)
	// concuRSwWP(f1)
	te1 := time.Now().Sub(ts1)

	// lab3
	ts3 := time.Now()
	concuRead2(f1)
	// print_records(records)
	te3 := time.Now().Sub(ts3)

	// Read and Set to a map
	fmt.Println("\nEND Basic: ", te)
	fmt.Println("END Concu: ", te1)

	fmt.Println("END Concu lab3: ", te3)
}

// with Worker pools
func concuRSwWP(f *os.File) {
	fcsv := csv.NewReader(f)
	rs := make([]*Sales, 0)
	numWps := 100
	jobs := make(chan []string, numWps)
	res := make(chan *Sales)

	var wg sync.WaitGroup
	worker := func(jobs <-chan []string, results chan<- *Sales) {
		for {
			select {
			case job, ok := <-jobs: // you must check for readable state of the channel.
				if !ok {
					return
				}
				results <- parseStruct(job)
			}
		}
	}

	// init workers
	for w := 0; w < numWps; w++ {
		wg.Add(1)
		go func() {
			// this line will exec when chan `res` processed output at line 107 (func worker: line 71)
			defer wg.Done()
			worker(jobs, res)
		}()
	}

	go func() {
		for {
			rStr, err := fcsv.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				fmt.Println("ERROR: ", err.Error())
				break
			}
			jobs <- rStr
		}
		close(jobs) // close jobs to signal workers that no more job are incoming.
	}()

	go func() {
		wg.Wait()
		close(res) // when you close(res) it breaks the below loop.
	}()

	for r := range res {
		rs = append(rs, r)
	}

	fmt.Println("Count Concu ", len(rs))
}

func concuRS(f *os.File) {
	fcsv := csv.NewReader(f)
	rs := make(map[int64]*Sales)

	var wg sync.WaitGroup
	for {
		rStr, err := fcsv.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("ERROR: ", err.Error())
			break
		}
		wg.Add(1)
		go func(pwg *sync.WaitGroup) {
			defer pwg.Done()
			obj := parseStruct(rStr)
			mu.Lock()
			rs[obj.OrderId] = obj
			mu.Unlock()
		}(&wg)
	}
	wg.Wait()
	fmt.Println("Count Concu ", len(rs))
}

func basicRS(f *os.File) {
	fcsv := csv.NewReader(f)
	rs := make([]*Sales, 0)
	for {
		rStr, err := fcsv.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("ERROR: ", err.Error())
			break
		}
		rs = append(rs, parseStruct(rStr))
	}
	fmt.Println("Count Basic ", len(rs))
}

func basicRead(f *os.File) {
	fcsv := csv.NewReader(f)
	for {
		rStr, err := fcsv.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("ERROR: ", err.Error())
			break
		}
		printData(rStr, "BS")
	}
}

func concuRead(f *os.File) {
	fcsv := csv.NewReader(f)
	for {
		rStr, err := fcsv.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("ERROR: ", err.Error())
			break
		}
		go printData(rStr, "CC")
	}
}

func printData(data []string, job string) {
	obj := parseStruct(data)
	js, _ := json.Marshal(obj)
	fmt.Printf("\n[%v] ROW Id: %v - len %v", job, obj.OrderId, len(js))
}

func parseStruct(data []string) *Sales {
	id, _ := strconv.ParseInt(data[6], 10, 64)
	unitSold, _ := strconv.ParseInt(data[8], 10, 64)
	unitPrice, _ := strconv.ParseFloat(data[9], 64)
	unitCost, _ := strconv.ParseFloat(data[10], 64)
	totalRev, _ := strconv.ParseFloat(data[11], 64)
	totalCost, _ := strconv.ParseFloat(data[12], 64)
	totalProfit, _ := strconv.ParseFloat(data[13], 64)
	return &Sales{
		Region:        data[0],
		Country:       data[1],
		ItemType:      data[2],
		SaleChannel:   data[3],
		OrderPriority: data[4],
		OrderDate:     data[5],
		OrderId:       id,
		ShipDate:      data[7],
		UnitSold:      unitSold,
		UnitPrice:     unitPrice,
		UnitCost:      unitCost,
		TotalRevenue:  totalRev,
		TotalCost:     totalCost,
		TotalProfit:   totalProfit,
	}
}

func concuRead2(file *os.File) {
	go func() {
		parser := csv.NewReader(file)

		defer close(records)
		for {
			record, err := parser.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				fmt.Println("ERROR: ", err.Error())
				break
			}

			records <- record
		}
	}()

}

func print_records(records chan []string) {
	for record := range records {
		fmt.Println(record)
	}
}
