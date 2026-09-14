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
TCP_NODELAY is on by default (`Transport.DisableTCPNoDelay` is false). Unix sockets that are  
world-writable (other-write `002`) are rejected unless `UnixSocketAnyMode`.

### Transport

TCP is the default (`host:port`). Unix sockets use `unix:///path/to.sock` and
avoid the local TCP stack when application and memcached share a host.
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

`VersionAll` and `StatsAll` hit every node. `Version` and `Stats` hit the first
live node (first `Servers()` entry not in `DeadServers()`).
`Admin().On(server).Flush` hits one node; `All().Flush` is the
broadcast. Ops live on `On` / `All`, not on the `Admin` factory.
`SetServers` and `Cluster.ResolverRefresh` require `Config.AllowSetServers`. UDP clients cannot mix with
TCP/Unix and cannot mutate. UDP allows Get/`gets`, Version, `Meta().Get` without
side-effect flags (`N`/`R`/`T`/`E`), `Meta().Noop`, and `Meta().Debug`.
`GetOpts.Touch` (`gat`/`gats`), `Stats`, and `StatsAll` are TCP-only (`ErrNotSupported` on UDP).
UDP also allows `VersionAll`.

### Security

Memcached text without TLS is a full data and admin plane.

**Flush** invalidates every item on the targeted servers (wire `flush_all`). Call
`Admin().All().Flush` or `On(server).Flush` after `Config.AllowAdmin`
**and** `Config.AllowFlushAll`.

**Admin** (`watch`, `metadump`, `mgdump`, `shutdown`, slabs/LRU) can exfiltrate
keys or take a node down. Enable only with `Config.AllowAdmin` and call
`client.Admin()`. Node-local commands require `On(server)`; `Shutdown` cannot
run without `On`. `All()` is Verbosity, CacheMemLimit, and Flush.
Watch and dump streams require a context deadline.

**UDP** has no integrity; request IDs help but the path is spoofable.

Without `AllowFlushAll`, `All().Flush` / `On().Flush` return
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
	"log"
	"time"

	"github.com/EyJunge1/go-memcached/memcached"
)

func main() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.Store(ctx, memcached.Item{Key: []byte("key"), Value: []byte("value")}); err != nil {
		log.Fatal(err)
	}
	var got memcached.GetResult
	if err := client.GetOne(ctx, []byte("key"), &got, memcached.GetOpts{}); err != nil {
		log.Fatal(err)
	}
	_ = got
}
```

CAS or touch is an option on `Get`, not a second verb: `Get(ctx, keys, dst, memcached.GetOpts{ReturnCAS: true})`.

Address lists with unix/udp/weight use `ParseServers` (not required for the happy path above):

```go
servers, err := memcached.ParseServers("10.0.0.1:11211?weight=2,10.0.0.2:11211")
```

Meta recache (`Vivify` / `Recache` / `Won`) is shown in `ExampleMetaClient_Get_recache`.

### Values and flags

The client stores and returns `[]byte` and a `uint32` flags field as-is. Encode (JSON, Protobuf, …) or compress in your application before `Store`; decode after `Get`. The library does not pull serialization or compression dependencies.

Keys are `[]byte` (1–250 bytes, no whitespace or control characters). Meta commands with a binary-key flag skip the whitespace check; the Base64 form on the wire must still be ≤250 bytes. `Get` does not copy keys into `GetResult`; use `keys[i]`. `Config.Cluster.Namespace` is applied on the wire only.

Expiration is `Exptime`: `0` never expires; 1..2592000 are relative seconds; larger values are Unix timestamps (32-bit; `UnixExp` rejects values above `MaxInt32`). A negative value expires immediately (`protocol.txt`; the client sends the token as-is). Use `Rel(d)` or `UnixExp(t)`. `flush_all` `FlushOpts.Delay` nil omits the token (immediate) and is not the same as item `0` (never). Recache window `R` is remaining TTL seconds (`time.Duration`), not an item `Exptime` and not capped at 30 days. `MetaGet.Recache==0` omits `R`.

`Get` fills `dst[i]` for `keys[i]`. A miss leaves `Found` false and is not `ErrNotFound`. An empty stored value is `Found` with `len(Value)==0`. Reuse `dst` and a Value buffer with enough cap for the zero-allocation path; `GetOne` is the single-key helper.

`Get` splits keys across cluster nodes and fetches in parallel. `Cluster.FanoutWorkers` enables persistent bounded workers in very large clusters; zero keeps the minimum-latency goroutine-per-node path. The default Ketama picker returns its node index directly, including failover-filtered snapshots; custom internal pickers safely use the compatibility lookup. `GetOpts.ReturnCAS` selects `gets`/`gats`; `GetOpts.Touch` selects `gat`/`gats`. `Store` returns `ErrNotStored` / `ErrExists` / `ErrNotFound` via `errors.Is`. Text `noreply` (`Item.Quiet`, `MutateOpts.Quiet` on Delete/Touch, `FlushOpts.Quiet`) discards the connection and still returns `CLIENT_ERROR`/`SERVER_ERROR` when the server already replied. Text `Incr`/`Decr` always return the new value; quiet arithmetic is `Meta().Arithmetic` plus `q`. `AdminOpts.Quiet` is send-ok only (the server sends nothing once `noreply` is parsed). `Pipeline()` queues Store/Delete/Touch/Incr/Decr/Get/GetOne, writes every text frame, then reads every reply (`Run`; no text `noreply`). Store followed by Get for the same key therefore takes one write/read wave but is not atomic: Get still executes after a rejected Store. Queued keys are copied; Store values are borrowed and must remain unchanged until `Run` returns. Optional, distinct `PipelineResult` destinations distinguish `Done`, `Invalid`, `Unknown`, and `NotExecuted`; `Unknown` mutations must not be retried blindly. `Pipeline.Prepare` copies Store values and retains validation, encoded frames, and routing for repeated non-concurrent runs; `PreparedBatch.RunValues` borrows replacement storage values in enqueue order, including variable lengths. Call `PreparedBatch.Close`. `Meta().PreparePipeline` similarly owns a reusable quiet meta batch, including per-node wire payloads and persistent opaque indexes; `PreparedMetaBatch.RunValues` borrows replacement Meta Set bodies. Prepared routing rebuilds reuse their grouping and wire scratch after warmup. There is no operation-count cap: larger same-node batches still use one write/read wave, bounded by memory and server limits. `Client.Session(ctx, key)` leases one routed stream connection; its convenience methods use that connection directly. `Client.NewSessionPool(ctx, key, size)` preopens a fixed set for parallel workers. Sessions are sequential, do not fail over, and reject keys routed to another node. `Session.RunPrepared` and `Session.RunPreparedValues` combine prepared plans with a dedicated connection. Quiet meta stays on `Meta().Pipeline` (`q` plus `mn`). Meta structs accept `ProxyHint`/`ProxyPath` (wire `P`/`L`; a `MetaOp` with `Noop` can put them on the pipeline barrier). Health is `Version` / `VersionAll` plus the error. `SetServers` replaces the node list when `AllowSetServers` is set; it cannot switch UDP and stream.

### Support Commands

Current memcached **text + meta** is fully covered (typed API, no raw command
passthrough). Deprecated binary-era commands are listed after the table and
are **not** implemented.

| Command                                     | Status | API Usage                                                                                | Description                                                                                                                                                                     |
| ------------------------------------------- | ------ | ---------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `set` / `add` / `replace` / `cas`           | ✅      | `Store` + `Item.Mode`                                                                    | flags, `Exptime`, bytes; `noreply` via `Item.Quiet`                                                                                                                             |
| `append` / `prepend`                        | ✅      | `Store` + `StoreAppend` / `StorePrepend`                                                 | wire still has Storage-grammar `0 0` (server ignores them)                                                                                                                      |
| `get` / `gets` / `gat` / `gats`             | ✅      | `Get` / `GetOne` + dest + `GetOpts`                                                      | `Get` fills `dst` index-aligned; miss is `!Found`; `ReturnCAS` → `gets`/`gats`; `Touch` → `gat`/`gats` (TCP-only)                                                             |
| `delete` / `incr` / `decr` / `touch`        | ✅      | typed; `MutateOpts.Quiet` on delete/touch                                                | `Quiet` → `noreply`; Incr/Decr always return the number; quiet arith via `Meta().Arithmetic` + `q`                                                                              |
| `mg` / `ms` / `md` / `ma` / `me` / `mn`     | ✅      | `Meta().Get` / `Set` / `Delete` / `Arithmetic` / `Debug` / `Noop` + dest                  | every documented request flag (`b c C f F h k l O q s t u v E N R T I M J D x P L`) and response extras (`W X Z`); statuses HD/VA/EN/NF/NS/EX/MN/ME; `q` always pairs with `mn` |
| Recache                                     | ✅      | `MetaGet.Vivify` / `MetaGet.Recache` / `MetaDelete.Invalidate`                           | `mg N` / `mg R` (Duration, no 30-day cap) / `md I T`                                                                                                                            |
| `Pipeline` / `Meta().Pipeline`              | ✅      | `Pipeline().Store/GetOne/…Run` / `Meta().Pipeline`                                       | text is write-then-read with optional per-op results (no `noreply`); quiet meta stays on `Meta().Pipeline`                                                                      |
| Prepared batch / session                    | ✅      | `Pipeline().…Prepare` / `Meta().PreparePipeline` / `Client.Session` / `Client.NewSessionPool` | reusable owned text/meta plans, dynamic text values, and leased single-node connections; each plan/session is sequential, pools provide parallel leases                      |
| `stats` + subcommands                       | ✅      | `Stats` / `StatsAll` + dest; Admin `On().StatsSizes` / `StatsReset`; `detail` via `StatsDetail` | no args, `items`, `slabs`, `settings`, `conns`; TCP-only                                                                                                             |
| `version`                                   | ✅      | `Version` / `VersionAll` + dest                                                          | `Version` first live node; `VersionAll` every node                                                                                                                              |
| `flush_all`                                 | ✅      | `Admin().On().Flush` / `All().Flush`                                                     | `AllowAdmin` + `AllowFlushAll`; `Delay`/`Stagger`; `Quiet` → `noreply`                                                                                                          |
| `cache_memlimit` / `verbosity`              | ✅      | `Admin().On()` / `All()`                                                                 | MB + `noreply`; level + `noreply`                                                                                                                                               |
| `shutdown`                                  | ✅      | `Admin().On()`                                                                           | default or `graceful`; not on `All()`                                                                                                                                           |
| `slabs` / `lru` / `lru_crawler`             | ✅      | `Admin().On()`                                                                           | `reassign` (src `-1`); `SlabsAutomove` Standby/Normal/Aggressive; `LRUTune` / `LRUMode` / `LRUTempTTL`; enable/disable/sleep/tocrawl/crawl; `metadump`/`mgdump` (`all`/`hash`) |
| `watch`                                     | ✅      | `Admin().On().Watch`                                                                     | `fetchers` `mutations` `evictions` `connevents` `proxyreqs` `proxyevents` `proxyuser` `deletions`; stackable                                                                    |
| Token auth `-Y`                             | ✅      | `Config.Auth`                                                                            | fake `set` on every new dial; leftover probe uses a random `__mc_auth_*` key                                                                                                    |
| UDP framing                                 | ✅      | `udp://` / `udp4` / `udp6`                                                               | 8-byte header, one request datagram, reserved=0; Get/`gets`/Version/`VersionAll`/`me`/`mn` and `mg` without `N`/`R`/`T`/`E`                                                      |
| `quit`                                      | ✅      | `Close`                                                                                  | TCP writes `quit` then closes; UDP skips `quit` (spec allows close-only)                                                                                                        |
| `SetServers`                                | ✅      | requires `AllowSetServers`                                                               | replaces the node list                                                                                                                                                          |
| `Admin()`                                   | opt-in | `On(server)` / `All()`                                                                   | all admin/ops above; `Shutdown` and node-local ops need `On`                                                                                                                    |

Deprecated or removed — **not implemented** (current memcached does not offer these as a client API, or the spec replacement is already in the table):

| Command                                     | Status     | Why not                                                                                                                       |
| ------------------------------------------- | ---------- | ----------------------------------------------------------------------------------------------------------------------------- |
| Binary protocol                             | deprecated | Meta replaces it; see [https://docs.memcached.org/protocols/](https://docs.memcached.org/protocols/)                          |
| SASL                                        | deprecated | hangs on binary; text auth is `-Y` (`Config.Auth`)                                                                            |
| TAP / VBucket / Range-Ops (`RGet`/`RSet`/…) | deprecated | binary / Couchbase; not in current text+meta `protocol.txt`                                                                   |
| `stats cachedump`                           | superseded | use `Admin().On().LRUCrawlerMetaDump` (no cache lock)                                                                         |
| `stats sizes_enable` / `sizes_disable`      | removed    | server dropped the runtime toggle (not thread-safe); start with `-o track_sizes`; read via `Admin().On().StatsSizes` |
| `misbehave`                                 | debug-only | seccomp self-test in debug builds, not an app command                                                                         |

#### Code Style

This project follows the standard Go code style guidelines and uses golangci-lint for additional checks, including **gosec**. Formatting is enforced by **gofumpt**. The configuration can be found in [.golangci.yml](./.golangci.yml).