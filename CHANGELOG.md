# Changelog

All notable changes to this project are documented in this file.

## [0.1.0] - 2026-09-14

### Added

- Text and meta protocol client with Pipeline, PreparedBatch, PreparedMetaBatch,
  Session, and SessionPool APIs.
- Weighted Ketama clustering, optional failover eject, TLS, Unix, and UDP
  (read-only) transports.
- Persistent fanout workers via `Cluster.FanoutWorkers`.
- Security policy (`SECURITY.md`) and pre-release audit documentation.

### Security

- Reject trailing bytes after complete response waves; discard the connection.
- Cap protocol lines at 64 KiB and stats responses at 4 MiB.
- Bound UDP fragment reassembly to `MaxValueBytes` plus protocol overhead;
  rotate UDP sockets when the 16-bit request id wraps.
- Reject duplicate meta-pipeline response opaques.
- Reject ASCII control characters in Auth credentials (including BEL/DEL).

### Fixed

- MaxOpen pool waiters wake after `discard` and no-idle `put`.
- `NewSessionPool` rejects `size > Pool.MaxOpen` instead of hanging.
- Persistent fanout no longer deadlocks `Client.Close` on a saturated queue.
- Pool-close races during client shutdown map to `ErrClosed`.
- Removed nodes cannot create zombie pools on first dial after `SetServers`.
- Retrieval destinations for a failed node wave are cleared on parse/transport errors.

### Notes

- First public tag is **v0.1.0**, not v1.0.0.
- `Client.Close` shuts idle sockets and cancels persistent fanout; it does not
  abort uncancellable checked-out single-node I/O (`TimeoutNone` +
  `context.Background()`).
