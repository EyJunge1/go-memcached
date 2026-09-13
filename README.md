# go-memcached

### Features

- Text- and meta-protocol for **application commands** (get/set/… and mg/ms/md/ma/me/mn).
  Admin ops (`verbosity`, `shutdown`, slabs, LRU, `watch`, dumps) exist only behind `Config.AllowAdmin` + `Admin()`.
- Values are raw `[]byte` plus `uint32` flags. Encode or compress in the caller.
- Cluster default is **weighted Ketama/MD5** (libmemcached / php-memcached). Use
  `NewCRC32HashPickBuilder` for gomemcache-style modulo hashing. murmur3 and
  rendezvous are also available.
- Connection pool per node via `Config.Pool`. Failover is on when more than one
  server is configured (`FailureLimit=2`, `RetryAfter=1s`, `DeadAfter=30s`,
  `Eject=true`).
- Transport: TCP (default, **plaintext**), `unix://`, `udp://`. TLS via `Config.Transport.TLS`.
  `Transport.RequireTLS` makes `New` fail without TLS; it does not flip the default.
  TCP_NODELAY is on by default (`Transport.TCPNoDelay`). Unix sockets that are
  world-writable (other-write `002`) are rejected unless `UnixSocketAnyMode`.

### Out of scope

- Binary protocol (deprecated; see https://docs.memcached.org/protocols/ )
- SASL (binary). Text token auth (`-Y`) is `Config.Auth` at dial time.
- TAP, VBucket, Range-Ops (`RGet`/`RSet`)
- Serialization or compression libraries

Unlike gomemcache and yeqown, SASL is not offered; `FlushAll` and Admin
require explicit `Config` flags.

### Transport

TCP is the default (`host:port`). Unix sockets use `unix:///path/to.sock`.
UDP addresses use `udp://host:port`. Framing is used only for `udp` / `udp4` / `udp6`.

```go
tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
servers, err := memcached.ParseServers("memcached.example.com:11211")
if err != nil {
	panic(err)
}
client, err := memcached.New(memcached.Config{
	Servers:   servers,
	Transport: memcached.Transport{TLS: tlsCfg},
})
```

`Transport.Dial` is the raw socket. If `Transport.TLS` is set, the client
always wraps that socket with `tls.Client` and handshakes — Dial is never a
silent plaintext escape. `InsecureSkipVerify` requires
`Transport.AllowInsecureTLS`. `MinVersion == 0` defaults to TLS 1.2.
UDP and TLS together are a `New` error. The default is plaintext: any on-path
party is a full cache admin.

### Cluster hashing

The default picker is weighted Ketama/MD5. CRC-32 IEEE modulo N is available
via `NewCRC32HashPickBuilder` for gomemcache interop — it is **not**
libmemcached `MEMCACHED_HASH_CRC`. murmur3 here is a 64-bit digest; libhashkit
murmur3 is 32-bit. Rendezvous (HRW) is also not Ketama.

Weights: `10.0.0.1:11211?weight=2` or `Server.Weight`.

`Ping`, `VersionAll`, and `StatsAll` hit every node. `Version` and `Stats`
hit one node. `Admin().FlushAll` hits every node; `On(addr).FlushAll` hits
one node; `All().FlushAll` is the explicit broadcast. `SetServers` requires
`Config.AllowSetServers`. UDP clients cannot mix with TCP/Unix and cannot
mutate. UDP allows Get/GetOne/GetMulti/gets, Version/Ping, MetaGet without
side-effect flags (`N`/`R`/`T`/`E`), MetaNoop, and MetaDebug.

### Security

Memcached text without TLS is a full data and admin plane.

**FlushAll** invalidates every item on every configured server. Call
`Admin().FlushAll` after `Config.AllowAdmin` **and** `Config.AllowFlushAll`.

**Admin** (`watch`, `metadump`, `mgdump`, `shutdown`, slabs/LRU) can exfiltrate
keys or take a node down. Enable only with `Config.AllowAdmin` and call
`client.Admin()`. Node-local commands require `On(addr)`; `Shutdown` cannot
run without `On`. `All()` is Verbosity, CacheMemLimit, Stats, and FlushAll.
Watch and dump streams require a context deadline.

**UDP** has no integrity; request IDs help but the path is spoofable.

Without `AllowFlushAll`, `Admin().FlushAll` returns `ErrFlushAllNotAllowed`
and does not send `flush_all`.

Token auth (`Config.Auth`) is rejected on plaintext TCP except Unix, loopback,
or `Auth.AllowPlaintext`. UDP + Auth is `ErrNotSupported`.

### Installation

```bash
go get github.com/EyJunge1/go-memcached/memcached@latest
```

### Usage

```go
package main

import (
	"context"
	"time"

	"github.com/EyJunge1/go-memcached/memcached"
)

func main() {
	servers, err := memcached.ParseServers("localhost:11211")
	if err != nil {
		panic(err)
	}
	client, err := memcached.New(memcached.Config{Servers: servers})
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	version, err := client.Version(ctx)
	if err != nil {
		panic(err)
	}
	println("Version: ", version)

	if err = client.Set(ctx, []byte("key"), []byte("value"), 0, memcached.ExptimeNever); err != nil {
		panic(err)
	}
	if err = client.Set(ctx, []byte("key2"), []byte("value2"), 0, memcached.ExptimeNever); err != nil {
		panic(err)
	}

	items, err := client.Get(ctx, memcached.GetOpts{CAS: true}, []byte("key"), []byte("key2"))
	if err != nil {
		panic(err)
	}

	for _, item := range items {
		println("key: ", string(item.Key), " value: ", string(item.Value))
	}
}
```

### Values and flags

The client stores and returns `[]byte` and a `uint32` flags field as-is. Encode (JSON, Protobuf, …) or compress in your application before `Set`; decode after `Get`/`GetOne`. The library does not pull serialization or compression dependencies.

Keys are `[]byte` (1–250 bytes, no whitespace or control characters). Meta commands with a binary-key flag skip the whitespace check; the Base64 form on the wire must still be ≤250 bytes. After a get, `Item.Key` is the **wire key**, including a configured namespace prefix.

Expiration is `Exptime`: `0` never expires; 1..2592000 are relative seconds; larger values are Unix timestamps (32-bit; `UnixExp` rejects values above `MaxInt32`). Negative values are a **client convention** (wire `-1`) that expire immediately; they are not in protocol.txt. Use `Rel(d)` or `UnixExp(t)`. `flush_all` delay `0` omits the token (immediate) and is not the same as item `0` (never). Recache window `R` is remaining TTL seconds (`time.Duration`), not an item `Exptime` and not capped at 30 days. `GetEarlyRecache(window<=0)` is `ErrInvalidArgument`; `MetaGet.Recache==0` omits `R`.

`GetOne` maps a miss to `ErrNotFound`. Multi-`Get` and `GetMulti` omit missing keys and never return `ErrNotFound` for an empty result.

`Get` and `GetMulti` split keys across cluster nodes and fetch in parallel. `GetOpts.CAS` selects `gets`/`gats`; `GetOpts.Touch` selects `gat`/`gats`. `Store`/`Add`/`Cas` return `ErrNotStored` / `ErrExists` / `ErrNotFound` via `errors.Is`. Text `noreply` (`StoreRequest.Quiet`, `MutateOpts.Quiet` on Delete/Touch, `IncrQuiet`/`DecrQuiet`, `FlushOpts.Quiet`, `AdminOpts.Quiet`) means send-ok only; the connection is discarded and wire errors are not observed. `Incr`/`Decr` reject `MutateOpts.Quiet`. `Pipeline()` writes every text frame, then reads every reply (no text `noreply`). Quiet meta stays on `MetaPipeline` (`q` plus `mn`). Recache helpers: `GetOrVivify`, `GetEarlyRecache`, `Invalidate`. Meta structs accept proxy hints `P`/`L` (`mn` included; a `MetaOpNoop` can put `P`/`L` on the pipeline barrier). `Ping` checks every node. `SetServers` replaces the node list when `AllowSetServers` is set.

### Support Commands

The public API exposes typed text- and meta-protocol commands only. There is no
arbitrary raw command passthrough.

| Command         | Status | API Usage | Description |
|-----------------|--------|-----------|-------------|
| Set             | ✅ | `Set` / `Store(StoreSet, …)` | `error`; Quiet only via `StoreRequest.Quiet` |
| Add / Replace / Cas | ✅ | typed + `Store` | Flags and `Exptime`; `ErrNotStored` / `ErrExists` / `ErrNotFound` |
| Append / Prepend | ✅ | `Append(ctx, key, value)` | No dummy flags/expiry |
| Get             | ✅ | `Get(ctx, GetOpts, keys...)` | Multi-key; miss omitted |
| GetOne          | ✅ | `GetOne(ctx, key)` | Single key; miss → `ErrNotFound` |
| GetMulti        | ✅ | `GetMulti(ctx, keys)` | Map with CAS; missing keys omitted |
| Delete / Incr / Decr / Touch | ✅ | typed + `MutateOpts` | Delete/Touch `Quiet` sends `noreply`; `IncrQuiet`/`DecrQuiet` for arith noreply |
| Pipeline        | ✅ | `Pipeline().Set/Delete; Flush` | Write-then-read; TTL via `Item.Exp`; append/prepend force `0 0`; no text `noreply` |
| Stats           | ✅ | `Stats(ctx, args...)` | Free tokens; `reset`/`sizes` only Admin; generic `Stats` rejects `detail` — use `Admin().StatsDetail` |
| StatsAll / VersionAll | ✅ | cluster maps | Every node; partial errors joined |
| Meta*           | ✅ | `MetaGet`/`MetaSet`/… structs | `MetaResult.Status` (HD/VA/EN/NF/NS/EX/MN); `me` via `MetaItemDebug` (`Key` decoded when `b`); `P`/`L` including `mn` |
| Recache         | ✅ | `GetOrVivify` / `GetEarlyRecache` / `Invalidate` | `mg N` / `mg R` (Duration, no 30d cap) / `md I T` |
| MetaPipeline    | ✅ | mixed ops + auto-`O` + `mn` | Parallel per node; Quiet-Miss is `EN` (mg) / `HD` (mutation); `MetaOpNoop` `P`/`L` on barrier |
| Version / Ping  | ✅ | | `Version` one node; `Ping` all |
| FlushAll        | ✅ | `Admin().FlushAll` / `On` / `All` | `AllowAdmin` + `AllowFlushAll`; `Delay`/`Exp`/`Stagger`; `Quiet` sends `noreply` |
| Auth            | ✅ | `Config.Auth` | Token auth `-Y` on every new dial; leftover probe uses a random `__mc_auth_*` key |
| SetServers      | ✅ | requires `AllowSetServers` | Trust-anchor |
| Admin()         | opt-in | `On(addr)` / `All()` | `watch`, dumps, slabs/lru, verbosity, shutdown, `cache_memlimit`, `StatsDetail`; Shutdown and node-local ops need `On` |

### Development Guide

#### Prerequisites

- Go 1.27 or higher
- Python (for pre-commit hooks) or just `brew install pre-commit` on MacOS
- Docker (for running memcached in tests)

#### Setting up development environment

1. Clone the repository:
    ```bash
    git clone https://github.com/EyJunge1/go-memcached.git
    cd go-memcached
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
