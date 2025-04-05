### install choco
```bat
Set-ExecutionPolicy Bypass -Scope Process -Force; [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072; iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))
```

### install protoc

choco install protoc

### install gRPC in Go

go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2

### Update your PATH so that the protoc compiler can find the plugins:

Add `%GOPATH%/bin` to your PATH environment variable

### Init module 

go mod init httt.dev/basic-grpc

go get -u github.com/golang/protobuf/protoc-gen-go

### Generate protobuff file
#### add below line before syntax line to generate with "basic-grpc" package name
#### if you have multi-proto file =>the same package name
```
option go_package = "httt.dev/basic-grpc";
```

#### run command 
```
protoc -I=proto --go_out=paths=source_relative:pb proto/*.proto

```

### install makefile

```
choco install make
```

### Tại dòng import thì vscode sẽ báo lỗi. Có thể chỉnh sửa bằng cách sửa path cho đúng hoặc thay đổi setting của protoc như sau :
### Thêm đoạn sau vào config của protoc.
### C:\\ProgramData\\chocolatey\\lib\\protoc\\tools\\bin  : path to protoc.exe
### --proto_path=proto : thiết lập thư mục chứa proto file 

```json
"protoc": {
        "path": "C:\\ProgramData\\chocolatey\\lib\\protoc\\tools\\bin",
                    "options": [
                        "--proto_path=proto"
                    ]
    }

```