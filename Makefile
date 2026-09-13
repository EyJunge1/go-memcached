MEMCACHED_IMAGE ?= 1.6.42
MEMCACHED_TCP ?= 127.0.0.1:11211
MEMCACHED_UDP ?= udp://127.0.0.1:11211
MEMCACHED_UNIX ?= unix:///tmp/memcached.sock
export PODMAN_COMPOSE_WARNING_LOGS := false

.PHONY: lint test coverage vuln check pre-commit podman-ready \
	live-up live-down live-test live-test-run bench-micro bench-compare clean

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

podman-ready:
	@command -v podman >/dev/null || { echo "missing podman"; exit 1; }
	@podman info >/dev/null 2>&1 || { echo "podman is not ready; run: podman machine init && podman machine start"; exit 1; }

LIVE_COMPOSE := podman compose -f livetest/compose.yaml
BENCH_COMPOSE := podman compose -f benchmark/compose.yaml

live-up: podman-ready
	@echo "Starting live memcached (memcached:$(MEMCACHED_IMAGE))"
	@MEMCACHED_IMAGE=$(MEMCACHED_IMAGE) $(LIVE_COMPOSE) up -d --wait --wait-timeout 90

live-down:
	@echo "Stopping live memcached"
	@$(LIVE_COMPOSE) down -v --remove-orphans || true
	@$(BENCH_COMPOSE) down -v --remove-orphans || true

live-test: live-up live-test-run

live-test-run:
	@echo "Running live tests"
	@cd livetest && MEMCACHED_TCP=$(MEMCACHED_TCP) MEMCACHED_UDP=$(MEMCACHED_UDP) \
		MEMCACHED_UNIX=$(MEMCACHED_UNIX) go test -v -race -count=1 ./...

bench-micro:
	@echo "Running offline ProtocolBuilder microbenchmarks"
	@go test -run=^$$ -bench=^BenchmarkProtocolBuilder -benchmem ./memcached/

bench-compare: podman-ready
	@echo "Starting bench memcached (memcached:$(MEMCACHED_IMAGE))"
	@MEMCACHED_IMAGE=$(MEMCACHED_IMAGE) $(BENCH_COMPOSE) up -d --wait --wait-timeout 90
	@echo "Running client comparison benchmarks"
	@cd benchmark && MEMCACHED_TCP=$(MEMCACHED_TCP) MEMCACHED_UDP=$(MEMCACHED_UDP) \
		go test -run=^$$ -bench=. -benchmem -count=10

clean:
	@echo "Cleaning build artifacts"
	@rm -f coverage.txt coverage.html
	@go clean -testcache
