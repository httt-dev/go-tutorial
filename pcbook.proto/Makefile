gen:
	protoc -I=proto --go_out=paths=source_relative:pb proto/*.proto

clean:
	del pb\*.go

run:
	go run main.go