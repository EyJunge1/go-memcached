MEMCACHED_IMAGE ?= 1.6.42
MEMCACHED_TCP ?= 127.0.0.1:11211
MEMCACHED_UDP ?= udp://127.0.0.1:11211
BENCHSTAT_OLD ?= results/old.txt
BENCHSTAT_NEW ?= results/new.txt
PPROF_FILE ?= results/cpu.pprof
PPROF_HTTP ?= :8080
export PODMAN_COMPOSE_WARNING_LOGS := false

# Go tool pins (latest as of update).
GOLANGCI_LINT := github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0
GOFUMPT := mvdan.cc/gofumpt@v0.12.0
GOIMPORTS := golang.org/x/tools/cmd/goimports@v0.52.0
GOVULNCHECK := golang.org/x/vuln/cmd/govulncheck@v1.9.0
GOSEC := github.com/securego/gosec/v2/cmd/gosec@v2.30.0
STATICCHECK := honnef.co/go/tools/cmd/staticcheck@v0.8.2
GOCYCLO := github.com/fzipp/gocyclo/cmd/gocyclo@v0.6.0
BENCHSTAT := golang.org/x/perf/cmd/benchstat@v0.0.0-20261009192801-be2c69fb417e
GORELEASE := golang.org/x/exp/cmd/gorelease@v0.0.0-20261113113407-e3970619a1a4
GOCYCLO_OVER ?= 30

BENCH_OUT ?= results/bench-all.txt
BENCH_COUNT ?= 10

.PHONY: lint fmt fmt-check imports imports-check vet gosec staticcheck gocyclo \
	test coverage vuln gorelease check podman-ready \
	bench-micro bench-compare bench-summary benchstat pprof clean memcachedotel-test memcachedotel-lint memcachedotel-vuln

lint:
	@echo "Running golangci-lint (core)"
	@go run $(GOLANGCI_LINT) run ./...
	@echo "Running golangci-lint (memcachedotel)"
	@cd memcachedotel && go run $(GOLANGCI_LINT) run ./...
	@echo "Running golangci-lint (benchmark)"
	@cd benchmark && go run $(GOLANGCI_LINT) run ./...

fmt:
	@echo "Running gofumpt"
	@go run $(GOFUMPT) -l -w .

fmt-check:
	@echo "Checking gofumpt"
	@out=$$(go run $(GOFUMPT) -l .); if [ -n "$$out" ]; then echo "$$out"; exit 1; fi

imports:
	@echo "Running goimports"
	@go run $(GOIMPORTS) -local github.com/EyJunge1/go-memcached -l -w .

imports-check:
	@echo "Checking goimports"
	@out=$$(go run $(GOIMPORTS) -local github.com/EyJunge1/go-memcached -l .); if [ -n "$$out" ]; then echo "$$out"; exit 1; fi

vet:
	@echo "Running go vet (core)"
	@go vet ./...
	@echo "Running go vet (memcachedotel)"
	@cd memcachedotel && go vet ./...
	@echo "Running go vet (benchmark)"
	@cd benchmark && go vet ./...

gosec:
	@echo "Running gosec (core)"
	@# G401/G501: Ketama/libmemcached continuum digests require MD5 (not crypto).
	@go run $(GOSEC) -exclude=G401,G501 ./...
	@echo "Running gosec (memcachedotel)"
	@cd memcachedotel && go run $(GOSEC) -exclude=G401,G501 ./...

staticcheck:
	@echo "Running staticcheck (core)"
	@go run $(STATICCHECK) ./...
	@echo "Running staticcheck (memcachedotel)"
	@cd memcachedotel && go run $(STATICCHECK) ./...

gocyclo:
	@echo "Running gocyclo (over $(GOCYCLO_OVER))"
	@go run $(GOCYCLO) -over $(GOCYCLO_OVER) \
		./memcached ./memcachedotel ./benchmark

test:
	@echo "Running tests (core)"
	@go test -v -race ./...
	@echo "Running tests (memcachedotel)"
	@cd memcachedotel && go test -v -race ./...

memcachedotel-test:
	@cd memcachedotel && go test -v -race ./...

memcachedotel-lint:
	@cd memcachedotel && go run $(GOLANGCI_LINT) run ./...

memcachedotel-vuln:
	@cd memcachedotel && go run $(GOVULNCHECK) ./...

coverage:
	@echo "Running tests with coverage (core)"
	@go test -v -race -coverprofile=coverage.txt -covermode=atomic ./...
	@go tool cover -html=coverage.txt -o coverage.html
	@echo "Running tests with coverage (memcachedotel)"
	@cd memcachedotel && go test -v -race -coverprofile=coverage.txt -covermode=atomic ./...

vuln:
	@echo "Running govulncheck (core)"
	@go run $(GOVULNCHECK) ./...
	@echo "Running govulncheck (memcachedotel)"
	@cd memcachedotel && go run $(GOVULNCHECK) ./...

# gorelease diffs HEAD and rejects a dirty worktree. Copy the tree without
# .git so the working tree is checked, including uncommitted API edits.
# memcachedotel replaces the unpublished core module; gorelease ignores replace, so
# that run starts once memcachedotel/go.mod requires a real core tag (vX.Y.Z).
gorelease:
	@set -euo pipefail; \
	src=$$(pwd); \
	tmp=$$(mktemp -d); \
	trap 'rm -rf "$$tmp"' EXIT; \
	tar -C "$$src" --exclude .git --exclude coverage.txt --exclude coverage.html -cf - . | tar -C "$$tmp" -xf -; \
	echo "Running gorelease (core)"; \
	( cd "$$tmp" && go run $(GORELEASE) ); \
	req=$$(cd "$$tmp/memcachedotel" && go list -m -f '{{.Version}}' github.com/EyJunge1/go-memcached); \
	if printf '%s\n' "$$req" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+$$'; then \
		echo "Running gorelease (memcachedotel)"; \
		( cd "$$tmp/memcachedotel" && go run $(GORELEASE) ); \
	else \
		echo "Skipping gorelease (memcachedotel): core require is $$req, need a release tag (vX.Y.Z)"; \
	fi

check: fmt-check imports-check vet lint gosec staticcheck gocyclo test vuln gorelease

podman-ready:
	@command -v podman >/dev/null || { echo "missing podman"; exit 1; }
	@podman info >/dev/null 2>&1 || { echo "podman is not ready; run: podman machine init && podman machine start"; exit 1; }

BENCH_COMPOSE := podman compose -f benchmark/compose.yaml

bench-micro:
	@echo "Running offline ProtocolBuilder microbenchmarks"
	@go test -run=^$$ -bench=^BenchmarkProtocolBuilder -benchmem ./memcached/

bench-compare: podman-ready
	@echo "Starting bench memcached (memcached:$(MEMCACHED_IMAGE))"
	@MEMCACHED_IMAGE=$(MEMCACHED_IMAGE) $(BENCH_COMPOSE) up -d --wait --wait-timeout 90
	@mkdir -p results
	@echo "Running client comparison benchmarks -> $(BENCH_OUT)"
	@cd benchmark && MEMCACHED_TCP=$(MEMCACHED_TCP) MEMCACHED_UDP=$(MEMCACHED_UDP) \
		go test -run=^$$ -bench=. -benchmem -count=$(BENCH_COUNT) | tee ../$(BENCH_OUT)
	@$(MAKE) bench-summary BENCH_OUT=$(BENCH_OUT)

bench-summary:
	@echo "Writing median/min/max package table into $(BENCH_OUT)"
	@cd benchmark && go run benchtable.go -append ../$(BENCH_OUT)

benchstat:
	@echo "Running benchstat ($(BENCHSTAT_OLD) vs $(BENCHSTAT_NEW))"
	@cd benchmark && go run $(BENCHSTAT) $(BENCHSTAT_OLD) $(BENCHSTAT_NEW)

pprof:
	@echo "Opening pprof UI on $(PPROF_HTTP) ($(PPROF_FILE))"
	@cd benchmark && go tool pprof -http=$(PPROF_HTTP) $(PPROF_FILE)

clean:
	@echo "Cleaning build artifacts"
	@rm -f coverage.txt coverage.html
	@go clean -testcache
