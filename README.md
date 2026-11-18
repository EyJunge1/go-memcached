# go-memcached

[![memcached](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fproxy.golang.org%2Fgithub.com%2F%21ey%21junge1%2Fgo-memcached%2Fmemcached%2F%40latest&query=%24.Version&label=memcached&logo=go&logoColor=white&color=00ADD8&style=flat-square)](https://pkg.go.dev/github.com/EyJunge1/go-memcached/memcached) [![memcachedotel](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fproxy.golang.org%2Fgithub.com%2F%21ey%21junge1%2Fgo-memcached%2Fmemcachedotel%2F%40latest&query=%24.Version&label=memcachedotel&logo=go&logoColor=white&color=00ADD8&style=flat-square)](https://pkg.go.dev/github.com/EyJunge1/go-memcached/memcachedotel) [![License](https://img.shields.io/github/license/EyJunge1/go-memcached?style=flat-square&color=blueviolet)](LICENSE)

Languages: **🇬🇧** | [🇨🇳](./.github/i18n/README.zh.md)

Concurrent Go client for memcached text and meta protocols.

### Install

```bash
go get github.com/EyJunge1/go-memcached/memcached@v0.2.0

# optional OpenTelemetry adapter (separate nested module / tag)
go get github.com/EyJunge1/go-memcached/memcachedotel@v0.2.0
```

| Module path | Import | Git tag |
| --- | --- | --- |
| `github.com/EyJunge1/go-memcached/memcached` | `…/memcached` | `memcached/vX.Y.Z` |
| `github.com/EyJunge1/go-memcached/memcachedotel` | `…/memcachedotel` | `memcachedotel/vX.Y.Z` |

Release the core tag (`memcached/v…`) first, then tag `memcachedotel/v…`. See `.github/workflows/release.yml`.

### Usage

```go
import "github.com/EyJunge1/go-memcached/memcached"

client, err := memcached.NewFromAddrs("localhost:11211")
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
```

Optional OpenTelemetry (separate module — wire your app SDK providers):

```go
obs, err := memcachedotel.New(
	memcachedotel.WithTracerProvider(otel.GetTracerProvider()),
	memcachedotel.WithMeterProvider(otel.GetMeterProvider()),
)
if err != nil {
	log.Fatal(err)
}
defer obs.Close()

cfg := memcached.Config{Servers: []memcached.Server{{Address: "localhost:11211"}}}
obs.Apply(&cfg) // sets Observer + pool hooks

client, err := memcached.New(cfg)
if err != nil {
	log.Fatal(err)
}
defer client.Close()
_ = obs.RegisterPoolStats(client) // occupancy gauges
```

### Support commands (full text + meta coverage)

Current memcached **text + meta** is fully covered (typed API, no raw command
passthrough). Optional `Extra []string` / meta `WithExtra` appends raw wire
tokens for forward-compatible server flags.

| Command                                     | Status | API Usage                                                                                | Description                                                                                                                                                                     |
| ------------------------------------------- | ------ | ---------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `set` / `add` / `replace` / `cas`           | ✅      | `Store` + `Item.Mode`                                                                    | flags, `Exptime`, bytes; `noreply` via `Item.Quiet`                                                                                                                             |
| `append` / `prepend`                        | ✅      | `Store` + `StoreAppend` / `StorePrepend`                                                 | wire still has Storage-grammar `0 0` (server ignores them)                                                                                                                      |
| `get` / `gets` / `gat` / `gats`             | ✅      | `Get` / `GetOne` + dest + `GetOpts`                                                      | `Get` fills `dst` index-aligned; miss is `!Found`; `ReturnCAS` → `gets`/`gats`; `Touch` → `gat`/`gats` (TCP-only)                                                             |
| `delete` / `incr` / `decr` / `touch`        | ✅      | typed; `MutateOpts` / `ArithmeticOpts`                                                   | `Quiet` → `noreply`; Incr/Decr take `dst *uint64` + `ArithmeticOpts`; quiet arith via `Meta().Arithmetic` + `q`; `Extra` before noreply                                      |
| `mg` / `ms` / `md` / `ma` / `me` / `mn`     | ✅      | `Meta().Get`/`GetOne` / `Set`/`Store` / `Delete` / `Arithmetic` / `Debug` / `Noop` + dest | documented flags + `Extra`/`WithExtra` escape hatch; response extras (`W X Z`); statuses HD/VA/EN/NF/NS/EX/MN/ME; `q` always pairs with `mn`                                 |
| Recache                                     | ✅      | `MetaGet.Vivify` / `MetaGet.Recache` / `MetaDelete.Invalidate`                           | `mg N` / `mg R` (Duration, no 30-day cap) / `md I T`                                                                                                                            |
| `Pipeline` / `Meta().Pipeline`              | ✅      | `Pipeline().Store/GetOne/…Run` / `Meta().Pipeline`                                       | text is write-then-read with optional per-op results (no `noreply`); quiet meta stays on `Meta().Pipeline`                                                                      |
| Prepared batch / session                    | ✅      | `Pipeline().…Prepare` / `Meta().PreparePipeline` / `Client.Session` / `Client.NewSessionPool` | reusable owned text/meta plans, dynamic text values, and leased single-node connections; each plan/session is sequential, pools provide parallel leases                      |
| `stats` + subcommands                       | ✅      | `Stats` / `StatsAll` + dest; Admin `On().StatsSizes` / `StatsReset`; `detail` via `StatsDetail` | no args, `items`, `slabs`, `settings`, `conns`; TCP-only                                                                                                             |
| `version`                                   | ✅      | `Version` / `VersionAll` + dest                                                          | `Version` first live node; `VersionAll` every node                                                                                                                              |
| `flush_all`                                 | ✅      | `Admin().On().Flush` / `All().Flush`                                                     | `AllowAdmin` + `AllowFlushAll`; `Delay`/`Stagger`; `Quiet` → `noreply`                                                                                                          |
| `cache_memlimit` / `verbosity`              | ✅      | `Admin().On()` / `All()`                                                                 | MB + `noreply`; level + `noreply`                                                                                                                                               |
| `shutdown`                                  | ✅      | `Admin().On()`                                                                           | `ShutdownImmediate` / `ShutdownGraceful`; not on `All()`                                                                                                                        |
| `slabs` / `lru` / `lru_crawler`             | ✅      | `Admin().On()`                                                                           | `reassign` (src `-1`); `SlabsAutomove` Standby/Normal/Aggressive; `LRUTune` / `LRUMode` / `LRUTempTTL`; enable/disable/sleep/tocrawl/crawl; `metadump`/`mgdump` (`all`/`hash`) |
| `watch`                                     | ✅      | `Admin().On().Watch`                                                                     | `fetchers` `mutations` `evictions` `connevents` `proxyreqs` `proxyevents` `proxyuser` `deletions`; stackable                                                                    |
| Token auth `-Y`                             | ✅      | `Config.Auth`                                                                            | fake `set` on every new dial; leftover probe uses a random `__mc_auth_*` key                                                                                                    |
| UDP framing                                 | ✅      | `udp://` / `udp4` / `udp6`                                                               | 8-byte header, one request datagram, reserved=0; Get/`gets`/Version/`VersionAll`/`me`/`mn` and `mg` without `N`/`R`/`T`/`E`                                                      |
| `quit`                                      | ✅      | `Close`                                                                                  | TCP writes `quit` then closes; UDP skips `quit` (spec allows close-only)                                                                                                        |
| `SetServers`                                | ✅      | requires `AllowSetServers`                                                               | replaces the node list                                                                                                                                                          |
| `Admin()`                                   | opt-in | `On(server)` / `All()`                                                                   | all admin/ops above; `Shutdown` and node-local ops need `On`                                                                                                                    |
