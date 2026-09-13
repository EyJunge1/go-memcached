// Package memcached is a thread-safe memcached client for Store and Get
// (text protocol) plus Meta().Get and related meta commands.
//
// Application commands:
//   - set/add/replace/append/prepend/cas via Store (zero Mode is set;
//     NOT_STORED/EXISTS/NOT_FOUND via errors.Is)
//   - get / gets / gat / gats via Get + GetOpts; missing keys are omitted
//   - Item.Quiet / MutateOpts.Quiet / FlushOpts.Quiet send noreply
//     (conn discarded; CLIENT_ERROR still returned). AdminOpts.Quiet is send-ok only.
//   - delete and touch (noreply via MutateOpts.Quiet)
//   - incr/decr always return the new value; quiet arithmetic is Meta().Arithmetic + q
//   - mg/ms/md/mn/ma/me via client.Meta() (Get/Set/Delete/Noop/Arithmetic/Debug/Pipeline)
//   - version via Version / VersionAll; stats via Stats / StatsAll
//     (reset/sizes only via Admin; detail via AdminNode.StatsDetail)
//   - flush_all via Admin().On / All Flush (requires Config.AllowFlushAll)
//
// Two pipeline worlds: Client.Pipeline is text write-then-read;
// Meta().Pipeline is the meta batch (quiet q plus mn).
//
// Admin commands (verbosity, shutdown, slabs, lru*, watch, crawler dumps)
// are available only through Client.Admin after Config.AllowAdmin.
// Cluster.ResolverRefresh requires AllowSetServers, as SetServers does.
//
// Values are opaque []byte. Serialize or compress in the caller before Store.
//
// The default picker is weighted Ketama/MD5.
//
// The default transport is plaintext TCP. Transport.RequireTLS fails New
// without TLS; it does not change that default. UDP is udp:// / udp4 / udp6
// only (8-byte framing, one request datagram). It cannot mix with TCP, Unix,
// TLS, or Auth, and it cannot mutate: Get/gets, Version, Meta().Get without
// N/R/T/E, Meta().Noop, Meta().Debug. GetOpts.Touch and StatsAll are TCP-only.
// Token auth (-Y) is configured on Config.Auth at dial time. SASL is out of
// scope. Binary, TAP, VBucket, and range ops are out of scope.
package memcached
