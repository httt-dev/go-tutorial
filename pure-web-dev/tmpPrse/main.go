package main

import (
	"html/template"
	"net/http"
)

var tpl *template.Template

func main() {
	// func ParseGlob(pattern string) ( *Template, error )

	tpl, _ = template.ParseGlob("templates/*.html")
	// can use pointer to template
	// tpl, _ = tpl.ParseGlob("templates/*.html")

	// Sử dụng HandlerFunc để chuyển đổi hàm tùy chỉnh thành Handler.
	indexHandler := http.HandlerFunc(indexHandler)
	// Đăng ký Handler với đường dẫn "/hello".
	http.Handle("/", indexHandler)

	// http.HandleFunc("/", indexHandler)
	http.HandleFunc("/about", aboutindexHandler)
	http.HandleFunc("/contact", contactHandler)
	http.HandleFunc("/login", loginHandler)

	http.ListenAndServe(":8080", nil)
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	tpl.ExecuteTemplate(w, "index.html", nil)
}

func aboutindexHandler(w http.ResponseWriter, r *http.Request) {
	tpl.ExecuteTemplate(w, "about.html", nil)
}

func contactHandler(w http.ResponseWriter, r *http.Request) {
	tpl.ExecuteTemplate(w, "contact.html", nil)
}

func loginHandler(w http.ResponseWriter, r *http.Request) {
	tpl.ExecuteTemplate(w, "login.html", nil)
}
