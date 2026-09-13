// Package memcached provides a thread-safe memcached text+meta client.
//
// Application commands:
//   - set/add/replace/append/prepend/cas (NOT_STORED/EXISTS/NOT_FOUND via errors.Is; Pipeline for write-then-read)
//   - StoreRequest.Quiet / MutateOpts.Quiet (Delete/Touch) / IncrQuiet / DecrQuiet / FlushOpts.Quiet / AdminOpts.Quiet send noreply (send-ok only; conn discarded)
//   - get / gets / gat / gats via Get + GetOpts; GetOne for a single-key miss
//   - ms/mg/md/mn/ma/me, including proxy hints P/L and recache helpers
//   - MetaNoop returns MetaResult (MN); MetaDebug sets Status (ME/EN) and decodes b keys
//   - delete, incr/decr, touch (Delete/Touch noreply via MutateOpts.Quiet)
//   - version, stats (free args; reset/sizes only via Admin; detail rejected — use Admin.StatsDetail)
//   - flush_all via Admin().FlushAll / On / All (also requires Config.AllowFlushAll; Quiet sends noreply)
//
// Admin commands (verbosity, shutdown, slabs, lru*, watch, crawler dumps)
// are available only through Client.Admin after Config.AllowAdmin.
//
// Values are opaque []byte. Serialize or compress in the caller before Set.
//
// Cluster mode defaults to weighted Ketama/MD5. Use NewCRC32HashPickBuilder
// for gomemcache-style modulo hashing. murmur3 and rendezvous are also
// available. Mixing the default picker with a CRC32-only peer places keys
// on different nodes.
//
// The default transport is plaintext TCP. Transport.RequireTLS fails New
// without TLS; it does not change that default. Token auth (-Y) is configured
// on Config.Auth at dial time; it is not SASL. Binary, TAP, VBucket, and
// range ops are out of scope.
package memcached
