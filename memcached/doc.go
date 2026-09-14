// Package memcached implements a concurrent text- and meta-protocol client.
//
// Application surface:
//   - Store sends set, add, replace, append, prepend, or cas (zero Mode is set).
//     Check NOT_STORED, EXISTS, and NOT_FOUND with errors.Is.
//   - Get fills a caller-provided []GetResult aligned with the keys. A miss
//     leaves Found false and is not ErrNotFound. GetOne writes one slot.
//   - Signatures take dest before options: GetOne(ctx, key, dst, opts).
//     Meta matches: Get(ctx, key, dst, flags).
//   - Quiet on Item, MutateOpts, and FlushOpts adds noreply. The connection is
//     discarded after send; CLIENT_ERROR is still returned. AdminOpts.Quiet
//     means only that the write was sent.
//   - Delete and Touch take MutateOpts.Quiet for noreply.
//   - Incr and Decr always return the new counter. Quiet arithmetic is
//     Meta().Arithmetic with q.
//   - Meta() exposes mg, ms, md, mn, ma, me (Get, Set, Delete, Noop,
//     Arithmetic, Debug, Pipeline). Each command writes into a caller
//     dest; reuse dest for the zero-allocation path. Transport and parse
//     failures are the method error. EN, NF, NS, and EX live on dst.Status
//     / dst.Err(); the method error stays nil.
//   - Version / VersionAll and Stats / StatsAll write into caller dests.
//     StatsReset and StatsSizes are on Admin; detail is AdminNode.StatsDetail.
//     Stats and StatsAll are TCP-only.
//   - flush_all is Admin().On or All Flush and requires Config.AllowFlushAll.
//
// Client.Pipeline is a text write-then-read batch. Meta().Pipeline is the
// meta batch; quiet ops pair q with mn.
//
// Verbosity, shutdown, slabs, lru*, watch, and crawler dumps are reached
// only through Client.Admin when Config.AllowAdmin is set. SetServers and
// Cluster.ResolverRefresh both need AllowSetServers.
//
// Values are raw []byte. Encoding and compression belong in the caller.
//
// Node selection defaults to weighted Ketama/MD5.
//
// The default path is plaintext TCP. Transport.RequireTLS rejects New when
// TLS is missing; it does not change that default. UDP (udp://, udp4, udp6)
// uses 8-byte framing and one request per datagram. It cannot share a Client
// with TCP, Unix, TLS, or Auth, and it cannot mutate. Allowed UDP calls:
// Get/gets, Version, VersionAll, Meta().Get without N/R/T/E, Meta().Noop,
// Meta().Debug. GetOpts.Touch, Stats, and StatsAll require TCP. Config.Auth
// is token auth (-Y) at dial. SASL, binary, TAP, VBucket, and range ops
// are not implemented.
package memcached
