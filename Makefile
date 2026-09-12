.PHONY: lint test coverage vuln check pre-commit docker-up docker-down clean

lint:
	@echo "Running golangci-lint"
	@go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.2 run ./...

test:
	@echo "Running tests"
	@go test -v -race ./...

coverage:
	@echo "Running tests with coverage"
	@go test -v -race -coverprofile=coverage.txt -covermode=atomic ./...
	@go tool cover -html=coverage.txt -o coverage.html

vuln:
	@echo "Running govulncheck"
	@go run golang.org/x/vuln/cmd/govulncheck@latest ./...

check: lint test vuln

pre-commit:
	@echo "Running pre-commit"
	@pre-commit run --all-files

docker-up:
	@echo "Starting memcached container"
	@docker run -d --name memcached-test -p 11211:11211 memcached:latest

docker-down:
	@echo "Stopping memcached container"
	@docker stop memcached-test
	@docker rm memcached-test

clean:
	@echo "Cleaning build artifacts"
	@rm -f coverage.txt coverage.html
	@go clean -testcache
