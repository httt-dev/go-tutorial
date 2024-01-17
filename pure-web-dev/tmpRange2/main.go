package main

import (
	"html/template"
	"net/http"
)

type task struct {
	Name string
	Done bool
}

var tpl *template.Template

// todo list to be exported

var Todo []task

func main() {

	// Todo = []task{{"task1", true}, {"task2", false}, {"task3", false}}
	Todo = []task{{"task1", true}, {"task2", true}, {"task3", true}}
	tpl, _ = tpl.ParseGlob("templates/*.html")
	http.HandleFunc("/todo", todoHandler)
	http.ListenAndServe(":8080", nil)
}

func todoHandler(w http.ResponseWriter, r *http.Request) {
	tpl.ExecuteTemplate(w, "todolist.html", Todo)
}
