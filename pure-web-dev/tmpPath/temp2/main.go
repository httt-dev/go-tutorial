package main

import (
	"html/template"
	"net/http"
)

var tpl *template.Template

func main() {
	// func ParseFiles( filename ...string ) ( *template.Template , error )

	// tpl, _ = template.ParseFiles("index1.html")

	// tpl, _ = template.ParseFiles("data1/index2.html")

	// tpl, _ = template.ParseFiles("data1/data2/index3.html")

	// tpl, _ = template.ParseFiles("../index4.html")

	tpl, _ = tpl.ParseFiles("../index4.html")

	http.HandleFunc("/", indexHandler)
	http.ListenAndServe("localhost:8080", nil)
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	// func (tpl *template.Template) Execute(wr io.Writer , data interface{}) error
	tpl.Execute(w, nil)
}
