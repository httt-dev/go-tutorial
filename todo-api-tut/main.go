package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type todo struct {
	ID        string `json:"id"`
	Item      string `json:"title"`
	Completed bool   `json:"completed"`
}

var todos = []todo{
	{ID: "1", Item: "Item 1", Completed: false},
	{ID: "2", Item: "Item 2", Completed: false},
	{ID: "3", Item: "Item 3", Completed: false},
	{ID: "4", Item: "Item 4", Completed: true},
}

func getTodos(context *gin.Context) {
	context.IndentedJSON(http.StatusOK, todos)
}

func main() {
	router := gin.Default()
	router.GET("/todos", getTodos)
	router.Run(":9090")

}
