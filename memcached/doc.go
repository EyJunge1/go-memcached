// Package memcached is a concurrent client for memcached's text and meta protocols.
//
// # API layers
//
// Core (usual path): New, Store, Get/GetOne, Delete, Touch, Incr/Decr, Meta()
// single ops, Version/Stats, and Config.Pool / Cluster / Transport. Text Get/Store
// stays first-class for adoption and debugging; Meta() is recommended for
// greenfield work that needs flags (recache, quiet, opaque, binary keys).
// Text and meta are both supported indefinitely upstream; only the binary
// protocol is deprecated.
//
// Advanced: Client.Pipeline and Pipeline.Prepare / PreparedBatch,
// Meta().Pipeline and Meta().PreparePipeline / PreparedMetaBatch
// (also On().Meta().PreparePipeline, pinned),
// Client.Session / NewSessionPool, and Admin() (AllowAdmin). Optional pool
// hooks (Pool.OnAcquire / OnCreate), Pool.SharedStreams (opt-in hot streams:
// meta opaque mux with write-while-read across callers; text ordered
// coalesce after meta drain), Config.Observer for per-op timing
// (text/meta/pipeline/admin/stats/watch/dumps), Cluster.MaxPipelineOps
// chunking, Rendezvous+XXH3 cluster routing, and
// Cluster.MemorystoreDiscovery (config get cluster) live here too.
// OpenTelemetry lives in the separate github.com/EyJunge1/go-memcached/memcachedotel
// module. These add batching, leased connections, and ops-plane commands; they
// account for much of the package size alongside the dual protocol stacks—not
// dead code.
//
// Create a Client with New or NewFromAddrs. Store and GetOne are the usual
// path: values are raw []byte, keys hash onto the cluster with Rendezvous+XXH3,
// and a Get miss leaves Found false rather than returning
// ErrNotFound. Sentinel aliases (ErrCASConflict, ErrCacheMiss, ErrMalformedKey)
// ease migration without duplicating the core verbs. Methods take context
// first, then required arguments, then a caller
// dst, then options (including text Incr/Decr → dst *uint64). Reuse dst to
// avoid allocations. Check store outcomes with errors.Is. Example shows a
// complete program. Meta().GetOne aliases Get; Meta().Store maps Item onto
// Set. Meta().GetMulti / StoreMulti / DeleteMulti are the quiet multi helpers
// (DeleteMulti requires DeleteMissIgnore or DeleteMissStrict; the zero mode is
// invalid).
//
// Ownership: Store/Item.Value and MetaOp.Body are borrowed until the call (or
// Pipeline.Run) returns. GetResult.Value and MetaItem.Value remain valid until
// that destination slot is filled again. On a retrieval parse/transport error
// for one node wave, that wave's destination slots are cleared; successful
// slots from other nodes may still be populated when the joined error is
// non-nil (documented partial success). Meta().Pipeline keeps per-opaque
// siblings on CLIENT_ERROR/SERVER_ERROR, but clears the node wave on
// ErrMalformedResponse or transport failure.
//
// Close shuts idle pooled sockets and cancels persistent fanout workers. It
// does not abort already checked-out single-node calls, ephemeral fanout
// (FanoutWorkers == 0), VersionAll/StatsAll/Admin.All broadcasts, or Sessions
// when those operations use TimeoutNone and an uncancellable caller context.
// Callers still own and must close Sessions after Client.Close. SetServers(nil)
// clears membership; changing the ring remaps keys and does not migrate data.
//
// Meta() is the typed meta-protocol surface. Transport and parse failures are
// the method error; protocol misses and conflicts live on dst.
// Unknown response flags are ignored for forward compatibility; malformed
// values of known numeric fields are rejected. MetaGet/MetaSet/… accept
// literal structs or chained With* helpers (MetaGetValue().WithCAS()); both
// map to the same wire flags. Extra []string (WithExtra on meta builders)
// appends raw wire tokens after typed flags so new server options work without
// a library release; framing tokens b/q/O stay typed. Text Item/GetOpts/
// MutateOpts/FlushOpts/AdminOpts/ArithmeticOpts use the same Extra field
// before noreply (or after get keys), including Pipeline and Session text ops
// (Quiet remains rejected on Pipeline/Session).
// Client.On(server) pins text/meta key ops (including Incr/Decr and
// Meta Get/Set/Delete/Arithmetic/Debug/Noop/GetOrFill/Deferred) to one
// configured node. Meta().Deferred / On().Meta().Deferred enqueue quiet
// Get/Set/Delete/Arithmetic then flush once. Client.Pipeline is a text
// write-then-read batch for mixed mutations and retrievals. Optional
// PipelineResult destinations retain each operation's outcome.
// Pipeline.Prepare creates a reusable prepared execution plan;
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
