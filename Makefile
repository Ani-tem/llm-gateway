.PHONY: run test

run:
	go run ./cmd/gateway

test:
	go vet ./...
	go test -race ./...
