// Package memcached is a concurrent client for memcached's text and meta protocols.
//
// Create a Client with New. Store and GetOne are the usual path: values are
// raw []byte, keys hash onto the cluster with weighted Ketama/MD5, and a Get
// miss leaves Found false rather than returning ErrNotFound. Methods take
// context first, then required arguments, then a caller dst, then options.
// Reuse dst to avoid allocations. Check store outcomes with errors.Is.
// Example shows a complete program.
//
// Ownership: Store/Item.Value and MetaOp.Body are borrowed until the call (or
// Pipeline.Run) returns. GetResult.Value and MetaItem.Value remain valid until
// that destination slot is filled again. On a retrieval parse/transport error
// for one node wave, that wave's destination slots are cleared; successful
// slots from other nodes may still be populated when the joined error is
// non-nil (documented partial success).
//
// Close shuts idle pooled sockets and cancels persistent fanout workers. It
// does not abort an already checked-out single-node call with TimeoutNone and
// an uncancellable context. SetServers(nil) clears membership; changing the
// ring remaps keys and does not migrate data.
//
// Meta() is the typed meta-protocol surface. Transport and parse failures are
// the method error; protocol misses and conflicts live on dst.
// Client.Pipeline is a text write-then-read batch for mixed mutations and
// retrievals. Optional PipelineResult destinations retain each operation's
// outcome. Pipeline.Prepare creates a reusable prepared execution plan;
// PreparedBatch.RunValues borrows per-run storage values.
// Meta().PreparePipeline creates the corresponding quiet meta plan, and
// PreparedMetaBatch.RunValues borrows per-run set bodies. Client.Session leases
// one routed connection for direct sequential work; Client.NewSessionPool
// supplies fixed parallel Session leases. Cluster.FanoutWorkers optionally
// enables persistent, bounded multi-node workers.
//
// Admin commands, Flush, and membership changes are opt-in (AllowAdmin,
// AllowFlushAll, AllowSetServers). Admin().On targets one node; Admin().All
// broadcasts Flush, Verbosity, and CacheMemLimit. Shutdown is On only.
//
// The default transport is plaintext TCP. UDP cannot share a Client with
// TCP, Unix, TLS, or Auth, and it cannot mutate. Stats and Get with Touch
// require TCP. SASL, binary, TAP, VBucket, and range ops are not implemented.
// A custom Transport.Dial is a trust boundary: Auth plaintext policy still
// keys off Config.Servers, so Dial must not silently redirect remote.
// Unix socket mode checks are best-effort before connect (TOCTOU).
package memcached
