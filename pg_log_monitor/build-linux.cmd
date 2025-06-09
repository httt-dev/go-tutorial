:: script to build PGs use GoLand terminal and Go

:: ---------------- set ENV variables ---------------- 
set GOOS=linux
set GOARCH=amd64
::set GOPATH=%cd%;%cd%/../GOLIB
set BUILD_FLAG=-ldflags "-s -w"
set OUT_DIR=BUILD\Linux
set EXT=Linux.exe
set BUILD_CMD=go build 

%BUILD_CMD% %BUILD_FLAG% -o %OUT_DIR%\pg-log-monitor_linux
