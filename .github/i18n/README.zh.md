# go-memcached

[![memcached](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fproxy.golang.org%2Fgithub.com%2F%21ey%21junge1%2Fgo-memcached%2Fmemcached%2F%40latest&query=%24.Version&label=memcached&logo=go&logoColor=white&color=00ADD8&style=flat-square)](https://pkg.go.dev/github.com/EyJunge1/go-memcached/memcached) [![memcachedotel](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fproxy.golang.org%2Fgithub.com%2F%21ey%21junge1%2Fgo-memcached%2Fmemcachedotel%2F%40latest&query=%24.Version&label=memcachedotel&logo=go&logoColor=white&color=00ADD8&style=flat-square)](https://pkg.go.dev/github.com/EyJunge1/go-memcached/memcachedotel) [![License](https://img.shields.io/github/license/EyJunge1/go-memcached?style=flat-square&color=blueviolet)](../../LICENSE)

语言: [🇬🇧](../../README.md) | **🇨🇳**

面向 memcached 文本协议与 meta 协议的并发 Go 客户端。

### 安装

```bash
go get github.com/EyJunge1/go-memcached/memcached@v0.2.0

# 可选 OpenTelemetry 适配器（独立嵌套模块 / 标签）
go get github.com/EyJunge1/go-memcached/memcachedotel@v0.2.0
```

| 模块路径 | Import | Git 标签 |
| --- | --- | --- |
| `github.com/EyJunge1/go-memcached/memcached` | `…/memcached` | `memcached/vX.Y.Z` |
| `github.com/EyJunge1/go-memcached/memcachedotel` | `…/memcachedotel` | `memcachedotel/vX.Y.Z` |

先发布核心标签（`memcached/v…`），再打 `memcachedotel/v…`。参见 `.github/workflows/release.yml`。

### 用法

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

可选 OpenTelemetry（独立模块 — 接入应用侧 SDK Provider）：

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
obs.Apply(&cfg) // 设置 Observer + pool hooks

client, err := memcached.New(cfg)
if err != nil {
	log.Fatal(err)
}
defer client.Close()
_ = obs.RegisterPoolStats(client) // 连接池占用 gauges
```

### 支持的命令（完整 text + meta 覆盖）

当前 memcached **text + meta** 均已完整覆盖（类型化 API，无原始命令透传）。可选 `Extra []string` / meta `WithExtra` 可追加原始线级 token，以便向前兼容新的服务端标志。

| 命令 | 状态 | API 用法 | 说明 |
| --- | --- | --- | --- |
| `set` / `add` / `replace` / `cas` | ✅ | `Store` + `Item.Mode` | flags、`Exptime`、bytes；`noreply` 通过 `Item.Quiet` |
| `append` / `prepend` | ✅ | `Store` + `StoreAppend` / `StorePrepend` | 线上仍带 Storage 语法 `0 0`（服务端忽略） |
| `get` / `gets` / `gat` / `gats` | ✅ | `Get` / `GetOne` + dest + `GetOpts` | `Get` 按索引填充 `dst`；未命中为 `!Found`；`ReturnCAS` → `gets`/`gats`；`Touch` → `gat`/`gats`（仅 TCP） |
| `delete` / `incr` / `decr` / `touch` | ✅ | 类型化；`MutateOpts` / `ArithmeticOpts` | `Quiet` → `noreply`；Incr/Decr 使用 `dst *uint64` + `ArithmeticOpts`；静默算术用 `Meta().Arithmetic` + `q`；`Extra` 在 noreply 之前 |
| `mg` / `ms` / `md` / `ma` / `me` / `mn` | ✅ | `Meta().Get`/`GetOne` / `Set`/`Store` / `Delete` / `Arithmetic` / `Debug` / `Noop` + dest | 已文档化的 flags + `Extra`/`WithExtra` 逃生舱；响应 extras（`W X Z`）；状态 HD/VA/EN/NF/NS/EX/MN/ME；`q` 始终与 `mn` 配对 |
| Recache | ✅ | `MetaGet.Vivify` / `MetaGet.Recache` / `MetaDelete.Invalidate` | `mg N` / `mg R`（Duration，无 30 天上限）/ `md I T` |
| `Pipeline` / `Meta().Pipeline` | ✅ | `Pipeline().Store/GetOne/…Run` / `Meta().Pipeline` | text 为 write-then-read，可选按操作结果（无 `noreply`）；静默 meta 走 `Meta().Pipeline` |
| Prepared batch / session | ✅ | `Pipeline().…Prepare` / `Meta().PreparePipeline` / `Client.Session` / `Client.NewSessionPool` | 可复用的 text/meta 执行计划、动态 text values，以及租用的单节点连接；每个 plan/session 串行，pool 提供并行租约 |
| `stats` + 子命令 | ✅ | `Stats` / `StatsAll` + dest；Admin `On().StatsSizes` / `StatsReset`；`detail` 通过 `StatsDetail` | 无参数、`items`、`slabs`、`settings`、`conns`；仅 TCP |
| `version` | ✅ | `Version` / `VersionAll` + dest | `Version` 第一个存活节点；`VersionAll` 每个节点 |
| `flush_all` | ✅ | `Admin().On().Flush` / `All().Flush` | `AllowAdmin` + `AllowFlushAll`；`Delay`/`Stagger`；`Quiet` → `noreply` |
| `cache_memlimit` / `verbosity` | ✅ | `Admin().On()` / `All()` | MB + `noreply`；level + `noreply` |
| `shutdown` | ✅ | `Admin().On()` | `ShutdownImmediate` / `ShutdownGraceful`；不在 `All()` 上 |
| `slabs` / `lru` / `lru_crawler` | ✅ | `Admin().On()` | `reassign`（src `-1`）；`SlabsAutomove` Standby/Normal/Aggressive；`LRUTune` / `LRUMode` / `LRUTempTTL`；enable/disable/sleep/tocrawl/crawl；`metadump`/`mgdump`（`all`/`hash`） |
| `watch` | ✅ | `Admin().On().Watch` | `fetchers` `mutations` `evictions` `connevents` `proxyreqs` `proxyevents` `proxyuser` `deletions`；可叠加 |
| Token auth `-Y` | ✅ | `Config.Auth` | 每次新 dial 上伪造 `set`；残留探测使用随机 `__mc_auth_*` key |
| UDP framing | ✅ | `udp://` / `udp4` / `udp6` | 8 字节头、一个请求 datagram、reserved=0；Get/`gets`/Version/`VersionAll`/`me`/`mn` 以及不含 `N`/`R`/`T`/`E` 的 `mg` |
| `quit` | ✅ | `Close` | TCP 先写 `quit` 再关闭；UDP 跳过 `quit`（规范允许仅关闭） |
| `SetServers` | ✅ | 需要 `AllowSetServers` | 替换节点列表 |
| `Admin()` | 可选 | `On(server)` / `All()` | 以上全部 admin/ops；`Shutdown` 与节点本地操作需要 `On` |
