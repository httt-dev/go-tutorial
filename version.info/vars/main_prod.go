//go:build prod

package main

func init() {
	version = "production"
}

// go build -tags prod
