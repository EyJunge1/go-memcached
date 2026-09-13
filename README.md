# go-memcached

### Features

- Text- and meta-protocol for **application commands** (get/set/… and mg/ms/md/ma/me/mn).
  Admin ops (`verbosity`, `shutdown`, slabs, LRU, `watch`, dumps) exist only behind `Config.AllowAdmin` + `Admin()`.
- Values are raw `[]byte` plus `uint32` flags. Encode or compress in the caller.
- Cluster default is **weighted Ketama/MD5**.
- Connection pool per node via `Config.Pool`. Failover is off unless
  `Config.Failover` is set (`DefaultFailover()` is FailureLimit=2,
  RetryAfter=1s, DeadAfter=30s, Eject=true).
- Transport: TCP (default, **plaintext**), `unix://`, `udp://`. TLS via `Config.Transport.TLS`.
  `Transport.RequireTLS` makes `New` fail without TLS; it does not flip the default.
  TCP_NODELAY is on by default (`Transport.TCPNoDelay`). Unix sockets that are
  world-writable (other-write `002`) are rejected unless `UnixSocketAnyMode`.

### Out of scope

Deprecated binary-era commands (Binary, SASL, TAP, VBucket, Range-Ops) and
removed stats toggles are listed under Support Commands — they are not
implemented. Values stay raw `[]byte`; encode or compress in the caller.

SASL is out of scope; text auth is `-Y` (`Config.Auth`). `FlushAll` and Admin
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

The default picker is weighted Ketama/MD5.

Weights: `10.0.0.1:11211?weight=2` or `Server.Weight`.

`VersionAll` and `StatsAll` hit every node. `Version` hits one node.
`Admin().On(server).FlushAll` hits one node; `All().FlushAll` is the
broadcast. Unbound `Admin().FlushAll` returns `ErrAdminTargetRequired`.
`SetServers` requires `Config.AllowSetServers`. UDP clients cannot mix with
TCP/Unix and cannot mutate. UDP allows Get/`gets`, Version, MetaGet without
side-effect flags (`N`/`R`/`T`/`E`), MetaNoop, and MetaDebug.
`GetOpts.Touch` (`gat`/`gats`) and `StatsAll` are TCP-only (`ErrNotSupported` on UDP).

### Security

Memcached text without TLS is a full data and admin plane.

**FlushAll** invalidates every item on the targeted servers. Call
`Admin().All().FlushAll` or `On(server).FlushAll` after `Config.AllowAdmin`
**and** `Config.AllowFlushAll`.

**Admin** (`watch`, `metadump`, `mgdump`, `shutdown`, slabs/LRU) can exfiltrate
keys or take a node down. Enable only with `Config.AllowAdmin` and call
`client.Admin()`. Node-local commands require `On(server)`; `Shutdown` cannot
run without `On`. `All()` is Verbosity, CacheMemLimit, and FlushAll.
Watch and dump streams require a context deadline.

**UDP** has no integrity; request IDs help but the path is spoofable.

Without `AllowFlushAll`, `All().FlushAll` / `On().FlushAll` return
`ErrFlushAllNotAllowed` and do not send `flush_all`.

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

	if err = client.Store(ctx, memcached.Item{Mode: memcached.StoreSet, Key: []byte("key"), Value: []byte("value")}); err != nil {
		panic(err)
	}
	if err = client.Store(ctx, memcached.Item{Mode: memcached.StoreSet, Key: []byte("key2"), Value: []byte("value2")}); err != nil {
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

The client stores and returns `[]byte` and a `uint32` flags field as-is. Encode (JSON, Protobuf, …) or compress in your application before `Store`; decode after `Get`. The library does not pull serialization or compression dependencies.

Keys are `[]byte` (1–250 bytes, no whitespace or control characters). Meta commands with a binary-key flag skip the whitespace check; the Base64 form on the wire must still be ≤250 bytes. After a get, `Item.Key` is the **wire key**, including a configured namespace prefix.

Expiration is `Exptime`: `0` never expires; 1..2592000 are relative seconds; larger values are Unix timestamps (32-bit; `UnixExp` rejects values above `MaxInt32`). Negative values are a **client convention** (wire `-1`) that expire immediately; they are not in protocol.txt. Use `Rel(d)` or `UnixExp(t)`. `flush_all` delay `0` omits the token (immediate) and is not the same as item `0` (never). Recache window `R` is remaining TTL seconds (`time.Duration`), not an item `Exptime` and not capped at 30 days. `MetaGet.Recache==0` omits `R`.

`Get` omits missing keys and never returns `ErrNotFound` for an empty result.

`Get` splits keys across cluster nodes and fetches in parallel. `GetOpts.CAS` selects `gets`/`gats`; `GetOpts.Touch` selects `gat`/`gats`. `Store` returns `ErrNotStored` / `ErrExists` / `ErrNotFound` via `errors.Is`. Text `noreply` (`Item.Quiet`, `MutateOpts.Quiet` on Delete/Touch/Incr/Decr, `FlushOpts.Quiet`) discards the connection and still returns `CLIENT_ERROR`/`SERVER_ERROR` when the server already replied. `AdminOpts.Quiet` is send-ok only (the server sends nothing once `noreply` is parsed). `Pipeline()` writes every text frame, then reads every reply (`Run`; no text `noreply`; `Item.Quiet` is `ErrInvalidArgument`). Quiet meta stays on `MetaPipeline` (`q` plus `mn`). Meta structs accept proxy hints `P`/`L` (`mn` included; a `MetaOp` with `Noop` can put `P`/`L` on the pipeline barrier). Health is `VersionAll` plus its error. `SetServers` replaces the node list when `AllowSetServers` is set.

### Support Commands

Current memcached **text + meta** is fully covered (typed API, no raw command
passthrough). Deprecated binary-era commands are listed after the table and
are **not** implemented.

| Command | Status | API Usage | Description |
|---------|--------|-----------|-------------|
| `set` / `add` / `replace` / `cas` | ✅ | `Store` + `Item.Mode` | flags, `Exptime`, bytes; `noreply` via `Item.Quiet` |
| `append` / `prepend` | ✅ | `Store` + `StoreAppend` / `StorePrepend` | wire still has Storage-grammar `0 0` (server ignores them) |
| `get` / `gets` / `gat` / `gats` | ✅ | `Get` + `GetOpts` | multi-key in one request per node; miss omitted; `CAS` → `gets`/`gats`; `Touch` → `gat`/`gats` (TCP-only) |
| `delete` / `incr` / `decr` / `touch` | ✅ | typed + `MutateOpts` | `Quiet` → `noreply`; Incr/Decr return `(0, nil)` |
| `mg` / `ms` / `md` / `ma` / `me` / `mn` | ✅ | `MetaGet` / `MetaSet` / `MetaDelete` / `MetaArithmetic` / `MetaDebug` / `MetaNoop` | every documented request flag (`b c C f F h k l O q s t u v E N R T I M J D x P L`) and response extras (`W X Z`); statuses HD/VA/EN/NF/NS/EX/MN/ME; `q` always pairs with `mn` |
| Recache | ✅ | `MetaGet.Vivify` / `MetaGet.Recache` / `MetaDelete.Invalidate` | `mg N` / `mg R` (Duration, no 30-day cap) / `md I T` |
| `Pipeline` / `MetaPipeline` | ✅ | `Pipeline().Store/…Run` / mixed meta + auto-`O` + `mn` | text is write-then-read (no `noreply`); quiet meta stays on `MetaPipeline` |
| `stats` + subcommands | ✅ | `StatsAll`; Admin `On().Stats` for `sizes`/`reset`; `detail` via `StatsDetail` | no args, `items`, `slabs`, `settings`, `conns`; TCP-only |
| `version` | ✅ | `Version` / `VersionAll` | `Version` one node; `VersionAll` every node |
| `flush_all` | ✅ | `Admin().On().FlushAll` / `All().FlushAll` | `AllowAdmin` + `AllowFlushAll`; `Delay`/`Stagger`; `Quiet` → `noreply` |
| `cache_memlimit` / `verbosity` / `shutdown` | ✅ | `Admin().On()` / `All()` | MB + `noreply`; level + `noreply`; default or `graceful` |
| `slabs` / `lru` / `lru_crawler` | ✅ | `Admin().On()` | `reassign` (src `-1`), `automove` 0\|1\|2; `tune`/`mode`/`temp_ttl`; enable/disable/sleep/tocrawl/crawl; `metadump`/`mgdump` (`all`/`hash`) |
| `watch` | ✅ | `Admin().On().Watch` | `fetchers` `mutations` `evictions` `connevents` `proxyreqs` `proxyevents` `proxyuser` `deletions`; stackable |
| Token auth `-Y` | ✅ | `Config.Auth` | fake `set` on every new dial; leftover probe uses a random `__mc_auth_*` key |
| UDP framing | ✅ | `udp://` / `udp4` / `udp6` | 8-byte header, one request datagram, reserved=0; Get/`gets`/Version/`me`/`mn` and `mg` without `N`/`R`/`T`/`E` |
| `quit` | ✅ | `Close` | TCP writes `quit` then closes; UDP skips `quit` (spec allows close-only) |
| `SetServers` | ✅ | requires `AllowSetServers` | replaces the node list |
| `Admin()` | opt-in | `On(server)` / `All()` | all admin/ops above; `Shutdown` and node-local ops need `On` |

Deprecated or removed — **not implemented** (current memcached does not offer these as a client API, or the spec replacement is already in the table):

| Command | Status | Why not |
|---------|--------|---------|
| Binary protocol | deprecated | Meta replaces it; see https://docs.memcached.org/protocols/ |
| SASL | deprecated | hangs on binary; text auth is `-Y` (`Config.Auth`) |
| TAP / VBucket / Range-Ops (`RGet`/`RSet`/…) | deprecated | binary / Couchbase; not in current text+meta `protocol.txt` |
| `stats cachedump` | superseded | use `Admin().On().LRUCrawlerMetaDump` (no cache lock) |
| `stats sizes_enable` / `sizes_disable` | removed | server dropped the runtime toggle (not thread-safe); start with `-o track_sizes`; read via `Admin().Stats(ctx, "sizes")` |
| `misbehave` | debug-only | seccomp self-test in debug builds, not an app command |

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
