.PHONY: run test test-integration loadtest

run:
	go run ./cmd/gateway

test:
	go vet ./...
	go test -race ./...

# Also runs the tests that need the real Postgres from docker compose.
test-integration:
	TEST_DATABASE_URL=postgres://gateway:gateway@localhost:5434/gateway go test -race ./...

# Load test with k6 (runs in Docker, nothing to install). Start the gateway
# first with: go run ./cmd/gateway -config loadtest/config.json
loadtest:
	docker run --rm -i --add-host host.docker.internal:host-gateway \
		-e RATE -e DURATION grafana/k6 run - < loadtest/chat.js
