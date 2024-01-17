package main

import (
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"sync"
)

type jadgy struct {
	concurrency int // number of concurrent
	uri         string
	chunks      map[int][]byte
	err         error //
	opath       string
	*sync.Mutex // guards
}

func init() {
	log.SetOutput(os.Stdout)
	flag.CommandLine.SetOutput(os.Stdout)
}

func main() {
	sum, err := new()
}

func NewJadgy() (*jadgy, error) {
	c := flag.Int("c", 0, "number of concurrent connections")
	h := flag.Bool("h", false, "displays available flags")
	o := flag.String("o", "", "output path of downloaded file , default is same directory")

	flag.Parse()

	if *h {
		flag.PrintDefaults()
		fmt.Println("\nExample Usage - $GOBIN/summon -c 5 http://www.africau.edu/images/default/sample.pdf")
		return nil, nil
	}

	if *c <= 0 {
		*c = 1

	}

	if len(flag.Args()) <= 0 {
		return nil, fmt.Errorf("Please pass file url")
	}
	u := flag.Args()[0]
	uri, err := url.ParseRequestURI(u)
	if err != nil {
		return nil, fmt.Errorf("Passed URL is invalid")
	}
	sum := new(jadgy)
	sum.concurrency = *c
	sum.uri = uri.String()
	sum.chunks = make(map[int][]byte)
	sum.Mutex = &sync.Mutex{}

	if *o != "" {
		sum.opath = *o
	}

	return sum, nil
}
