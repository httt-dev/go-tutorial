package main

import (
	"fmt"
	"net/http"
)

func main() {
	// registers the handler function for the given pattern in the DefaultServeMux
	http.HandleFunc("/hello", helloHandleFunc)
	// Sử dụng goroutine để chạy máy chủ và đảm bảo in thông báo sau khi máy chủ đã được bắt đầu thành công.
	go func() {
		fmt.Println("Server is running on port 8080.")
		err := http.ListenAndServe(":8080", nil)
		if err != nil {
			fmt.Println("Error when starting server on port 8080: ", err)
		}
	}()

	// // ListenAndServe lstens on the TCP network address addr and then calls Serve
	// // with handler to handle requests on incoming connections
	// http.ListenAndServe(":8080", nil) //khong dung go routine
	// // entering nill implicitly uses DefaultServeMux
	// // ServeMux is an HTTP request multiplexer. It matches the URL of each incoming
	// // request against a list of registered patterns and calls the handler for the
	// // pattern that most closely matches the URL

	// Chờ để không kết thúc chương trình ngay sau khi máy chủ được bắt đầu.
	select {}

}

func helloHandleFunc(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "r.URL.Path: %s", r.URL.Path)
	fmt.Fprint(w, "Hello, world!")
}
