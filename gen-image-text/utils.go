package main

import (
	"fmt"
	"github.com/golang/freetype"
	"github.com/golang/freetype/truetype"
	"github.com/johnpili/go-text-to-image/models"
	"github.com/johnpili/go-text-to-image/page"
	"gopkg.in/yaml.v2"
	"html/template"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"
)

const (
	FreeMono   = "FreeMono.ttf"
	FreeSans   = "FreeSans.ttf"
	UbuntuMono = "UbuntuMono-R.ttf"
)

func renderPage(w http.ResponseWriter, r *http.Request, vm interface{}, basePath string, filenames ...string) {
	p := vm.(*page.Page)

	if p.Data == nil {
		p.SetData(make(map[string]interface{}))
	}

	if p.ErrorMessages == nil {
		p.ResetErrors()
	}

	if p.UIMapData == nil {
		p.UIMapData = make(map[string]interface{})
	}
	p.UIMapData["basePath"] = basePath
	templateFS := template.Must(template.New("base").ParseFS(views, filenames...))
	err := templateFS.Execute(w, p)
	if err != nil {
		log.Panic(err.Error())
	}
}

func getFontMap(f string) string {
	if f == "FreeSans" {
		return FreeSans
	} else if f == "FreeMono" {
		return FreeMono
	} else if f == "UbuntuMono" {
		return UbuntuMono
	}
	return UbuntuMono
}

func loadFont(fn string) (*truetype.Font, error) {
	fontFile = fmt.Sprintf("static/fonts/%s", getFontMap(fn))
	fontBytes, err := static.ReadFile(fontFile)
	if err != nil {
		return nil, err
	}
	f, err := freetype.ParseFont(fontBytes)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// This will handle the loading of config.yml
func loadConfiguration(a string, b *models.Config) {
	f, err := os.Open(a)
	if err != nil {
		log.Fatal(err.Error())
	}

	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(b)
	if err != nil {
		log.Fatal(err.Error())
	}
}

func generateRandomBytes(length int) []byte {
	s := ""
	for i := 33; i <= 126; i++ {
		s = s + fmt.Sprintf("%c", i)
	}
	rs := make([]byte, 0)
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < length; i++ {
		delta := rand.Intn(len(s))
		rs = append(rs, s[delta])
	}
	return rs
}
