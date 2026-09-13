// Package memcached provides a thread-safe memcached text+meta client.
//
// Application commands:
//   - set/add/replace/append/prepend/cas via Store (NOT_STORED/EXISTS/NOT_FOUND via errors.Is; Pipeline for write-then-read)
//   - Item.Quiet / MutateOpts.Quiet / FlushOpts.Quiet send noreply (conn discarded; CLIENT_ERROR still returned). AdminOpts.Quiet is send-ok only.
//   - get / gets / gat / gats via Get + GetOpts; missing keys are omitted
//   - ms/mg/md/mn/ma/me, including proxy hints P/L
//   - MetaNoop returns MetaResult (MN); MetaDebug sets Status (ME/EN) and decodes b keys
//   - delete, incr/decr, touch (noreply via MutateOpts.Quiet)
//   - version, stats (StatsAll for every node; reset/sizes only via Admin; detail rejected — use Admin.StatsDetail)
//   - flush_all via Admin().On / All (requires Config.AllowFlushAll; unbound FlushAll is ErrAdminTargetRequired; Quiet sends noreply)
//
// Admin commands (verbosity, shutdown, slabs, lru*, watch, crawler dumps)
// are available only through Client.Admin after Config.AllowAdmin.
//
// Values are opaque []byte. Serialize or compress in the caller before Store.
//
// The default picker is weighted Ketama/MD5.
//
// The default transport is plaintext TCP. Transport.RequireTLS fails New
// without TLS; it does not change that default. UDP is udp:// / udp4 / udp6
// only (8-byte framing, one request datagram). It cannot mix with TCP, Unix,
// TLS, or Auth, and it cannot mutate: Get/gets, Version, MetaGet without
// N/R/T/E, MetaNoop, MetaDebug. GetOpts.Touch and StatsAll are TCP-only.
// Token auth (-Y) is configured on Config.Auth at dial time. SASL is out of
// scope. Binary, TAP, VBucket, and range ops are out of scope.
package memcached
