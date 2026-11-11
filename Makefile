.PHONY: run test test-integration

run:
	go run ./cmd/gateway

test:
	go vet ./...
	go test -race ./...

# Also runs the tests that need the real Postgres from docker compose.
test-integration:
	TEST_DATABASE_URL=postgres://gateway:gateway@localhost:5434/gateway go test -race ./...
