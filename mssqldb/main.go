package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"
	"time"

	_ "github.com/microsoft/go-mssqldb"
)

// https://olontsev.io/microsoft-sql-server-and-go-comprehensive-guide/
func main() {
	//context
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	appSignal := make(chan os.Signal, 3)
	signal.Notify(appSignal, os.Interrupt)

	go func() {
		<-appSignal
		stop()
	}()

	// "Server=localhost,1435;Database=TdBlog;User Id=sa;Password=Abc12345;TrustServerCertificate=true"
	dsn := "sqlserver://SA:Abc12345@localhost:1435?database=tempdb"
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.SetConnMaxLifetime(0)
	db.SetMaxIdleConns(3)
	db.SetMaxOpenConns(3)
	// Open connection
	OpenDbConnection(ctx, db)
}

func OpenDbConnection(ctx context.Context, db *sql.DB) {
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		log.Printf("Unable to connect to database: %v", err)
	}
	log.Printf("Connected to database")
}
