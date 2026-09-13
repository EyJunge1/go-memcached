# Live tests

Hits a real memcached over the public client API: text storage/get/mutate,
meta (mg/ms/md/ma/me/mn), pipelines, stats/version, admin (flush, watch,
crawler dump), namespace, SetServers, UDP, and optional Unix.
This module is not part of `go test ./...` at the repo root.

## Live instance

The stack file is [compose.yaml](./compose.yaml) in this module. It is not part of the library package. From the repo root, `make live-up` calls `podman compose -f livetest/compose.yaml`. Without a server, tests skip.

```bash
podman machine start    # macOS, once
make live-test          # starts this stack, then `go test` here
make live-down
```

Defaults: `MEMCACHED_TCP=127.0.0.1:11211`, `MEMCACHED_UDP=udp://127.0.0.1:11211`, `MEMCACHED_UNIX=unix:///tmp/memcached.sock`. Override to point at another instance.

UDP and Unix tests skip when that listener is absent. Compose publishes TCP and UDP on 11211; it does not mount a Unix socket.
