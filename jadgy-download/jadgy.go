// package main

// import (
// 	"os"
// 	"sync"
// 	"time"
// )

// type downloader func(wg *sync.WaitGroup) error

// type jadgy struct {
// 	concurrency      int64       //No. of connections
// 	uri              string      //URL of the file we want to download
// 	isResume         bool        //is this a resumr request
// 	isRangeSupported bool        // if thisrequest supports range
// 	err              error       //used when error occurs inside a goroutine
// 	startTime        time.Time   // to track time took
// 	fileDetails      fileDetails // will hold the file related details
// 	metaData         meta        // will hold the meta data of the range and file details
// 	progressBar      progressBar // index => progress
// 	stop             chan error  //to handle stop signal from terminal
// 	aeparator        string      // store the path separator based on the OS
// 	*sync.RWMutex                //mutex to lock the maps with accessing it concurrently
// }

// type fileDetails struct {
// 	chunks map[int64]*os.File // map of part files we are creating

// }
