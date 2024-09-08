package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/microsoft/go-mssqldb"
)

// https://shahidyousuf.com/database-connection-pooling-example-in-golang
// https://golangbot.com/connect-create-db-mysql/
// https://betterstack.com/community/guides/scaling-go/sql-databases-in-go/
type ConnectionPool struct {
	// the underlying connection pool
	pool *sql.DB

	// the maximum number of connections in the pool
	maxConnections int

	// the current number of connections in the pool
	numConnections int

	// the mutex to synchronize access to the connection pool
	mutex *sync.Mutex
}

// NewConnectionPool creates a new ConnectionPool instance
func NewConnectionPool(dsn string, maxConnections int) (*ConnectionPool, error) {
	// create a new connection pool
	pool, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return nil, err
	}

	// set the maximum number of connections in the pool
	pool.SetMaxOpenConns(maxConnections)

	// create a new ConnectionPool instance
	p := &ConnectionPool{
		pool:           pool,
		maxConnections: maxConnections,
		numConnections: 0,
		mutex:          &sync.Mutex{},
	}

	return p, nil
}

// GetConnection acquires a connection from the pool
func (p *ConnectionPool) GetConnection() (*sql.DB, error) {
	// acquire the mutex lock
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// check if the pool is full
	if p.numConnections == p.maxConnections {
		return nil, fmt.Errorf("connection pool is full")
	}

	// increment the number of connections in the pool
	p.numConnections++

	// return a connection from the underlying pool
	return p.pool, nil
}

// ReleaseConnection releases a connection back to the pool
func (p *ConnectionPool) ReleaseConnection(conn *sql.DB) {
	// acquire the mutex lock
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// decrement the number of connections in the pool
	p.numConnections--
}

func main() {
	// create a new connection pool
	pool, err := NewConnectionPool("user:password@tcp(host:port)/dbname", 100)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.pool.Close()

	// acquire a connection from the pool
	conn, err := pool.GetConnection()
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()

	// use the connection as before.
	err = conn.PingContext(ctx)
	if err != nil {
		log.Printf("Errors %s pinging DB", err)
		return
	}

	log.Printf("Connected to DB successfully\n")
}
