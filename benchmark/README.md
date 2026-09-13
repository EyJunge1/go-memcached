# Client comparison benchmarks

Compares this library against [yeqown/memcached](https://github.com/yeqown/memcached), [bradfitz/gomemcache](https://github.com/bradfitz/gomemcache), and [rainycape/memcache](https://github.com/rainycape/memcache) on one Podman Compose memcached (TCP and UDP on `127.0.0.1:11211`).

`ns/op` is mostly RTT and the server. Use `allocs/op` and `B/op` as the client signal. Numbers are machine-specific; do not treat them as a gate.

This module is not part of `go test ./...` at the repo root. Live correctness tests live in [livetest](../livetest).

## Live instance

The stack file is [compose.yaml](./compose.yaml) in this module. It is not part of the library package. From the repo root, `make bench-compare` calls `podman compose -f benchmark/compose.yaml`.

```bash
podman machine start    # macOS, once
make bench-compare      # starts this stack, then `go test -bench` here
make live-down
```

Defaults: `MEMCACHED_TCP=127.0.0.1:11211`, `MEMCACHED_UDP=udp://127.0.0.1:11211`. Override to point at another instance. Without a server, tests and benches skip.

UDP cannot Set in this library. UDP benches seed keys over TCP, then Get / MetaGet over UDP.

## Offline microbenchmarks

No extra bench files in the library package. `make bench-micro` runs the ProtocolBuilder benches that already live in `memcached/protocol_builder_test.go` (same pattern as yeqown). This module is only the live client comparison.

```bash
make bench-micro
```

## benchstat

```bash
go install golang.org/x/perf/cmd/benchstat@latest
mkdir -p results
go test -run=^$ -bench=BenchmarkTCPSetGet/ours -benchmem -count=10 > results/ours.txt
go test -run=^$ -bench=BenchmarkTCPSetGet/bradfitz -benchmem -count=10 > results/bradfitz.txt
benchstat results/bradfitz.txt results/ours.txt
```

Profiles:

```bash
go test -run=^$ -bench=BenchmarkTCPSetGet/ours/small -benchmem \
  -cpuprofile=results/cpu.pprof -memprofile=results/mem.pprof
go tool pprof -http=:8080 results/cpu.pprof
```

## Who implements what

| Client | Set+Get | Get-only | Multi-Get 8/32 | CAS `gets` | Meta | Text/Meta pipeline | UDP Get |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ours | yes | yes | yes | yes | yes | yes | yes (seed via TCP) |
| yeqown | yes | yes | yes (`Gets`) | yes (`Gets`) | yes | no | yes if `udp://` works |
| bradfitz | yes | yes | yes (`GetMulti`) | yes (`Get` is `gets`) | no | no | no |
| rainycape | yes if binary is on | yes | yes (`GetMulti`) | Get only (no CAS field) | no | no | no |

rainycape speaks the deprecated binary protocol. The bench runs it when the server answers; otherwise it skips.
