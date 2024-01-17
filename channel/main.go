package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/bxcodec/faker/v3"
	"github.com/gorilla/mux"
)

// Book struct đại diện cho thông tin của một quyển sách
type Book struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	JAN       string `json:"jan"`
	Publisher string `json:"publisher"`
}

// PageSize mặc định cho trang
const PageSize = 10

// Tạo một slice để lưu trữ danh sách sách (fake data)
var bookList []Book

func init() {
	// Tạo fake data cho khoảng 2 triệu record
	for i := 1; i <= 10000; i++ {
		book := Book{
			ID:        strconv.Itoa(i),
			Name:      faker.Word(),
			JAN:       faker.UUIDDigit(),
			Publisher: faker.Word(),
		}
		bookList = append(bookList, book)
	}
}

// GetBooksHandler xử lý yêu cầu GET và trả về danh sách sách dựa trên các tham số
func GetBooksHandler(w http.ResponseWriter, r *http.Request) {
	// Lấy tham số từ URL
	pageSizeParam := r.URL.Query().Get("pagesize")
	pageParam := r.URL.Query().Get("page")

	// Chuyển đổi tham số sang số nguyên
	pageSize, err := strconv.Atoi(pageSizeParam)
	if err != nil || pageSize <= 0 {
		pageSize = PageSize
	}

	page, err := strconv.Atoi(pageParam)
	if err != nil || page <= 0 {
		page = 1
	}

	// Tính toán chỉ số bắt đầu và kết thúc của danh sách sách dựa trên trang và kích thước trang
	startIndex := (page - 1) * pageSize
	endIndex := startIndex + pageSize

	// Kiểm tra xem chỉ số kết thúc có vượt quá độ dài của danh sách không
	if endIndex > len(bookList) {
		endIndex = len(bookList)
	}

	// Trích xuất danh sách sách dựa trên chỉ số bắt đầu và kết thúc
	resultBooks := bookList[startIndex:endIndex]

	// Chuyển đổi danh sách sách thành JSON và gửi về client
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resultBooks)
}

func GetBooksWithStartIndexHandler(w http.ResponseWriter, r *http.Request) {
	// Lấy tham số từ URL
	pageSizeParam := r.URL.Query().Get("pagesize")
	startIndexParam := r.URL.Query().Get("startIndex")

	// Chuyển đổi tham số sang số nguyên
	pageSize, err := strconv.Atoi(pageSizeParam)
	if err != nil || pageSize <= 0 {
		pageSize = PageSize
	}

	startIndex, err := strconv.Atoi(startIndexParam)
	if err != nil || startIndex < 0 {
		startIndex = 0
	}

	// Tính toán chỉ số kết thúc của danh sách sách dựa trên startIndex và kích thước trang
	endIndex := startIndex + pageSize

	// Kiểm tra xem chỉ số kết thúc có vượt quá độ dài của danh sách không
	if endIndex > len(bookList) {
		endIndex = len(bookList)
	}

	// Trích xuất danh sách sách dựa trên chỉ số bắt đầu và kết thúc
	resultBooks := bookList[startIndex:endIndex]

	// Chuyển đổi danh sách sách thành JSON và gửi về client
	w.Header().Set("Content-Type", "application/json")

	rand.Seed(time.Now().UnixNano())
	n := rand.Intn(1) // n will be between 0 and 5

	time.Sleep(time.Duration(n) * time.Second)

	json.NewEncoder(w).Encode(resultBooks)
}

func main() {
	// Seed để tạo dữ liệu ngẫu nhiên động
	rand.Seed(time.Now().UnixNano())

	// Tạo router và xác định các xử lý cho các route
	router := mux.NewRouter()
	router.HandleFunc("/api/books", GetBooksHandler).Methods("GET")

	router.HandleFunc("/api/books-index", GetBooksWithStartIndexHandler).Methods("GET")
	// Khởi động server trong một goroutine
	go func() {
		port := 8080
		fmt.Printf("Server is running on :%d...\n", port)
		http.ListenAndServe(fmt.Sprintf(":%d", port), router)
	}()

	// Chờ một thời gian để server có thể chạy trong goroutine
	time.Sleep(time.Second * 2)

	// Do something else in the main goroutine if needed

	// Chờ một phím nhấn để kết thúc chương trình
	fmt.Println("Press any key to exit.")
	fmt.Scanln()
}
