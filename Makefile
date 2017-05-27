run:
	go run main.go -conf=example.json

run-yunsom:
	go run main.go -conf=yunsom.json

build:
	go build -o bin/mq-customer main.go
