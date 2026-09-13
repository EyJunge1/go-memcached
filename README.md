# gomemcached

### Features

- Text- and meta-protocol for **application commands** (get/set/… and mg/ms/md/ma/me/mn).
  Admin ops (`verbosity`, `shutdown`, slabs, LRU, `watch`, dumps) exist only behind `WithAdminCommands()` + `Admin()`.
- Values are raw `[]byte` plus `uint32` flags. Encode or compress in the caller.
- Cluster: CRC-32 IEEE modulo N (default, same family as bradfitz/gomemcache), murmur3, rendezvous, optional **Ketama** (`NewKetamaPickBuilder`, libmemcached-awesome 1.1.4 / php-memcached), or a custom `Picker`.
  Default CRC32 is **not** Ketama and **not** libmemcached `HASH_CRC`. Do not mix this client with php-memcached on the same cluster unless you opt into Ketama.
- Connection pool per node: `WithMaxConns`, `WithMaxIdleConns`, `WithMaxLifetime`, `WithMaxIdleTimeout`, optional `WithMinIdleConns`. No automatic failover unless `WithFailover` is set.
- Transport: TCP (default, **plaintext**), `unix://`, `udp://`. TLS via `WithTLS` or `WithDialContext`.

### Out of scope

- Binary protocol (deprecated; see https://docs.memcached.org/protocols/ )
- SASL (binary). Text token auth (`-Y`) is `WithAuth` / `Authenticate`.
- TAP, VBucket, Range-Ops (`RGet`/`RSet`)
- Serialization or compression libraries

### Transport

TCP is the default (`host:port`). Unix sockets use `unix:///path/to.sock`.
UDP addresses use `udp://host:port` and are only suitable for fire-and-forget
(see protocol.txt “UDP protocol”). `WithUDPEnabled()` is a no-op; framing is
used only for `udp` / `udp4` / `udp6`.

```go
tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
client, err := memcached.New("memcached.example.com:11211",
    memcached.WithTLS(tlsCfg),
)
```

`WithDialContext` remains the hook for proxies and mTLS. Do not set
`InsecureSkipVerify` except in tests. The default is plaintext: any on-path
party is a full cache admin.

### Cluster hashing

The default picker is CRC-32 IEEE modulo N — the same family as
bradfitz/gomemcache, **not** libmemcached `MEMCACHED_HASH_CRC` and **not**
Ketama/MD5. murmur3 here is a 64-bit digest; libhashkit murmur3 is 32-bit.
Rendezvous (HRW) is also not Ketama.

For php-memcached / libmemcached consistent hashing, use:

```go
memcached.New(addrs, memcached.WithPickBuilder(memcached.NewKetamaPickBuilder(memcached.KetamaOptions{
    Weighted: true,
})))
```

Weights: `10.0.0.1:11211?weight=2` or `NewAddrWeighted`. `Addr.Priority` is only
an HRW tie-break.

`Ping`, `FlushAll`, `VersionAll`, and `StatsAll` hit every node. `Version` and
`Stats` hit one node. `SetServers` is a privileged trust-anchor change.

`WithFailover` + CRC32 remaps every key when a node is ejected; prefer Ketama
or Rendezvous.

### Security

Memcached text without TLS is a full data and admin plane.

**FlushAll** invalidates every item on every configured server. It requires
`WithAllowFlushAll()`.

**Admin** (`watch`, `metadump`, `mgdump`, `shutdown`, slabs/LRU) can exfiltrate
keys or take a node down. Enable only with `WithAdminCommands()` and call
`client.Admin()`. Always cancel/timeout watch and dump streams.

**Unsafe unless you know the spec traps:**

- `WithNoReply()` — client-wide; the server may still send `CLIENT_ERROR`.
- UDP — no integrity; request IDs help but the path is spoofable.

Without `WithAllowFlushAll()`, `FlushAll` returns `ErrFlushAllNotAllowed` and
does not send `flush_all`.

### Installation

```bash
go get github.com/EyJunge1/gomemcached@latest
```

### Usage

```go
package main

import (
    "context"
    "time"

    "github.com/EyJunge1/gomemcached"
)

func main() {
	// 1. build client
	// addrs is a string, if you have multiple memcached servers, you can use comma to separate them.
	// e.g. "localhost:11211,localhost:11212,localhost:11213"
	addrs := "localhost:11211"

	// Options: WithDialTimeout, WithTLS, WithPickBuilder, ...
	client, err := memcached.New(addrs)
	if err != nil {
		panic(err)
	}

	// 2. use the client
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	version, err := client.Version(ctx)
	if err != nil {
		panic(err)
	}
	println("Version: ", version)

	if err = client.Set(ctx, "key", []byte("value"), 0, 0); err != nil {
		panic(err)
	}
	if err = client.Set(ctx, "key2", []byte("value2"), 0, 0); err != nil {
		panic(err)
	}

	items, err := client.Gets(ctx, "key", "key2")
	if err != nil {
		panic(err)
	}

	for _, item := range items {
		println("key: ", item.Key, " value: ", string(item.Value))
	}
}
```

### Values and flags

The client stores and returns `[]byte` and a `uint32` flags field as-is. Encode (JSON, Protobuf, …) or compress in your application before `Set`; decode after `Get`. The library does not pull serialization or compression dependencies.

Keys must be 1–250 bytes and must not contain whitespace or control characters. Meta commands with a binary-key flag skip the whitespace check; the Base64 form on the wire must still be ≤250 bytes.

Expiration: `0` never expires; durations ≤30 days are relative seconds; longer durations are sent as Unix timestamps (`now+ttl`).

Single-Get miss and an empty multi-`Gets` map to `ErrNotFound`. `GetMulti` omits missing keys.

`Gets`, `GetAndTouches`, and `GetMulti` split keys across cluster nodes and fetch in parallel. `GetAndTouches` is multi-key `gats` (with CAS). `Ping` checks every node. `SetServers` replaces the node list at runtime.

### Support Commands

The public API exposes typed text- and meta-protocol commands only. There is no
arbitrary raw command passthrough.

| Command         | Status | API Usage | Description |
|-----------------|--------|-----------|-------------|
| Set             | ✅ | `Set(ctx, key, value, flag, expiry, opts...)` | Optional `NoReply()` |
| Add / Replace / Append / Prepend / Cas | ✅ | same pattern | Storage commands |
| Gets            | ✅ | `Gets(ctx, keys...)` | With CAS; per-node parallel |
| GetMulti        | ✅ | `GetMulti(ctx, keys)` | Map; missing keys omitted |
| Get             | ✅ | `Get(ctx, key)` | Single key; miss → `ErrNotFound` |
| GetAndTouch     | ✅ | `GetAndTouch(ctx, expiry, key)` | Single-key `gat` |
| GetAndTouches   | ✅ | `GetAndTouches(ctx, expiry, keys...)` | Multi-key `gats` (CAS) |
| Delete / Incr / Decr / Touch | ✅ | typed methods | Incr/Decr + noreply → `(0, nil)` |
| Stats           | ✅ | `Stats(ctx)` | General-purpose `stats` (one node) |
| StatsArgs       | ✅ | `StatsArgs(ctx, args...)` | Allowlisted args only |
| StatsAll / VersionAll | ✅ | cluster maps | Every node; partial errors joined |
| Meta*           | ✅ | `MetaGet`/`MetaSet`/… | `mn` on the wire; modes `S/E/R/A/P`; `q` still reads errors |
| MetaGetPipeline | ✅ | quiet multi-mg + `mn` | |
| Version / Ping  | ✅ | | `Version` one node; `Ping` all |
| FlushAll        | ✅ | requires `WithAllowFlushAll()` | Destructive; all nodes |
| FlushAllAfter   | ✅ | delay / `FlushAllStaggered` | |
| Authenticate    | ✅ | `WithAuth` or `Authenticate` | Token auth `-Y` |
| SetServers      | ✅ | runtime node list | Trust-anchor |
| Admin()         | opt-in | `WithAdminCommands()` | Verbosity, Shutdown, slabs, LRU, Watch, dumps |

### Development Guide

#### Prerequisites

- Go 1.27 or higher
- Python (for pre-commit hooks) or just `brew install pre-commit` on MacOS
- Docker (for running memcached in tests)

#### Setting up development environment

1. Clone the repository:
    ```bash
    git clone https://github.com/EyJunge1/gomemcached.git
    cd gomemcached
    ```
2. Install pre-commit hooks:
    ```bash
    pip install pre-commit
    # or MacOS
    # brew install pre-commit

    pre-commit install
    ```
3. Quality checks use the Makefile (`make lint` downloads golangci-lint via `go run`; no global install required).

#### Make targets

```bash
make lint      # golangci-lint v2 (gofumpt, gosec, unused, staticcheck)
make test      # go test -race
make coverage  # tests + coverage.html
make vuln      # govulncheck
make check     # lint + test + vuln
```

#### Running Tests

```bash
make test
# or with coverage
make coverage
```

Live tests against `localhost:11211` skip automatically if no server is listening. Default `go test` is fake-server + unit tests.

#### Code Style

This project follows the standard Go code style guidelines and uses golangci-lint for additional checks, including **gosec**. Formatting is enforced by **gofumpt**. The configuration can be found in [.golangci.yml](./.golangci.yml).
