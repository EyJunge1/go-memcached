# Changelog

All notable changes to this project are documented in this file.

## [0.1.0] - 2026-09-14

### Added

- Text and meta protocol client with Pipeline, PreparedBatch, PreparedMetaBatch,
  Session, and SessionPool APIs.
- Weighted Ketama clustering, optional failover eject, TLS, Unix, and UDP
  (read-only) transports.
- Persistent fanout workers via `Cluster.FanoutWorkers`.
- Security policy (`SECURITY.md`) and pre-release audit documentation
  (`PRE_RELEASE_AUDIT.md`).

### Security

- Reject trailing bytes after complete response waves; discard the connection.
- Cap protocol lines at 64 KiB and stats responses at 4 MiB.
- Bound UDP fragment reassembly to `MaxValueBytes` plus protocol overhead;
  rotate UDP sockets when the 16-bit request id wraps, including before the
  wrap request itself reads.
- Reject duplicate, unknown, and missing meta-pipeline response opaques.
- Clear meta-pipeline node-wave destinations on malformed or transport failure.
- Reject duplicate VALUE frames for the same requested key.
- Reject ASCII control characters in Auth credentials (including BEL/DEL).
- Reject control characters and unusable lengths in `Cluster.Namespace`; cap
  Meta proxy hints at 4 KiB.
- Reject Unix socket symlinks in the pre-connect mode gate.
- Fail closed when Auth probe entropy is unavailable.
- Reject TLS versions below 1.2 and verify the connected loopback peer before
  sending plaintext Auth through the standard dialer.
- Bound server-provided protocol error text to 4 KiB.

### Fixed

- MaxOpen pool waiters wake after `discard` and no-idle `put`.
- `warmup` and Cleaner MinIdle dials respect `MaxOpen` and abort on pool close
  even with `DialTimeout: TimeoutNone`.
- `NewSessionPool` rejects `size > Pool.MaxOpen` instead of hanging.
- Persistent fanout no longer deadlocks `Client.Close` on a saturated queue.
- Pool-close races during client shutdown map to `ErrClosed`.
- Removed nodes cannot create zombie pools on first dial after `SetServers`;
  stale closed pool pointers are cleared.
- Concurrent pool return and close cannot strand an idle connection.
- Identical `SetServers` membership is a no-op and does not invalidate Sessions.
- Invalid known numeric Meta Debug fields are rejected as malformed.

### Notes

- First public tag is **v0.1.0**, not v1.0.0.
- `Client.Close` shuts idle sockets and cancels persistent fanout; it does not
  abort uncancellable checked-out single-node, ephemeral-fanout, broadcast, or
  Session I/O (`TimeoutNone` + `context.Background()`).
- Dedicated Watch/Dump streams belong to the caller context and require a
  deadline.