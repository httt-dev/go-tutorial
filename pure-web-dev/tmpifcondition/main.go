package main

import (
	"fmt"
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

var tpl *template.Template

type User struct {
	Name     string
	Language string
	Member   bool
}

var u User

func main() {

	u = User{"Hoa Nguyen", "Vietnamese", false}

	u = User{
		"Hoa Nguyen",
		"Spanish",
		true,
	}

	tpl, _ = tpl.ParseGlob("templates/*.html")
	http.HandleFunc("/", indexHandler)
	http.HandleFunc("/welcome", welcomeHandler)
	http.ListenAndServe(":8080", nil)

}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "Hello, world!")
}

func welcomeHandler(w http.ResponseWriter, r *http.Request) {
	tpl.ExecuteTemplate(w, "membership2.html", u)

}
