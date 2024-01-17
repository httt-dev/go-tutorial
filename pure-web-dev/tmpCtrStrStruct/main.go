package main

import (
	"html/template"
	"net/http"
)

type prodSpec struct {
	Size   string
	Weight float32
	Desc   string
}

type product struct {
	ProdID int
	Name   string
	Cost   float64
	Specs  prodSpec
}

var tpl *template.Template
var prod1 product

func main() {
	prod1 = product{
		ProdID: 15,
		Name:   "Product 15",
		Cost:   1000,
		Specs: prodSpec{
			Size:   "100x40x40 cm",
			Weight: 65,
			Desc:   "This is a simple product",
		},
	}

	tpl, _ = tpl.ParseGlob("templates/*.html")
	http.HandleFunc("/productinfo", productInfoHandler)
	http.ListenAndServe(":8080", nil)
}

func productInfoHandler(w http.ResponseWriter, r *http.Request) {
	// tpl.ExecuteTemplate(w, "productinfo.html", prod1)
	tpl.ExecuteTemplate(w, "productinfo2.html", prod1)
}
