package main

import (
	"html/template"
	"net/http"
)

//  {{/* a comment */}}	Defines a comment
/*
{{.}}	Renders the root element
{{.Name}}	Renders the “Name”-field in a nested element
{{if .Done}} {{else}} {{end}}	Defines an if/else-Statement
{{range .List}} {{.}} {{end}}	Loops over all “List” field and renders each using {{.}}
*/

type GroceryList []string

var tpl *template.Template
var g GroceryList

func main() {
	g = GroceryList{"milk", "eggs", "green breans", "cheese", "flour", "sugar", "salt", "pepper"}
	tpl, _ = tpl.ParseGlob("templates/*.html")
	http.HandleFunc("/list", listHandler)
	http.HandleFunc("/list2", list2Handler)
	http.ListenAndServe(":8080", nil)
}
func listHandler(w http.ResponseWriter, r *http.Request) {
	tpl.ExecuteTemplate(w, "groceries.html", g)
}
func list2Handler(w http.ResponseWriter, r *http.Request) {
	tpl.ExecuteTemplate(w, "groceries2.html", g)
}
