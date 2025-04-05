package main

import "fmt"

var version = "development"

func main() {
	fmt.Printf("Running version %s\n", version)
}

//  go build -ldflags "-X main.version=new_verion_value"

// neu muon them git commit id khi build go thi lam nhu sau
// go build -ldflags "-X main.version=$(git rev-parse HEAD)"
