package memcached

import (
	"crypto/tls"
	"os"
	"time"
)

// TimeoutNone leaves a Transport timeout unlimited aside from the caller's context.
// A zero DialTimeout, ReadTimeout, or WriteTimeout still selects the package default.
const TimeoutNone time.Duration = -1

type (
	// Config is the sole input to New.
	// Zero or nil turns optional features off (Failover, Auth). A pointer field
	// is used when 0 is a legal wire token (GetOpts.Touch, FlushOpts.Delay);
	// otherwise a zero value omits the flag (MetaGet.Recache). Numeric zeros
	// become package defaults (DialTimeout 3s); TimeoutNone skips the extra
	// deadline. Features that default on use an inverted bool (DisableTCPNoDelay).
	Config struct {
		Servers         []Server
		Transport       Transport
		Pool            Pool
		Cluster         Cluster
		Failover        Failover // zero disables failover; DefaultFailover restores the old multi-node policy
		Auth            *Auth
		AllowFlushAll   bool // must be set before Admin On/All Flush; otherwise flush_all is refused
		AllowAdmin      bool // must be set before Client.Admin
		AllowSetServers bool // must be set before SetServers and Cluster.ResolverRefresh
		MaxValueBytes   int  // 0 becomes 1 MiB; used for write checks and VALUE/VA reads
		// Observer receives per-op timing. Must be non-blocking; nil disables it.
		// Prefer the separate memcachedotel module over embedding OpenTelemetry here.
		Observer Observer
	}

	// Server names one cluster member.
	Server struct {
		// Network is empty/"tcp" by default, or unix, udp, and the dual-stack
		// forms tcp4, tcp6, udp4, udp6.
		Network string
		Address string
	}

	// Transport configures dialing, TLS, and I/O deadlines.
	// Zero timeouts become DialTimeout 3s and Read/WriteTimeout 5s; RetryAttempts 0 becomes 1.
	// TimeoutNone skips the extra deadline. Dial is the raw socket; a non-nil TLS wraps it.
	Transport struct {
		// TLS enables TLS. MinVersion 0 defaults to TLS 1.2; explicit versions
		// below TLS 1.2 are rejected.
		TLS           *tls.Config
		Dial          DialContextFunc
		DialTimeout   time.Duration // 0 becomes 3s; TimeoutNone skips the extra deadline
		ReadTimeout   time.Duration // 0 becomes 5s; TimeoutNone skips the extra deadline
		WriteTimeout  time.Duration // 0 becomes 5s; TimeoutNone skips the extra deadline
		KeepAlive     time.Duration // 0 leaves net.Dialer's keep-alive; not TimeoutNone
		RetryAttempts int           // 0 becomes 1
		// RetryBackoff is the base pause before retrying the same node after a
		// transport/network error. Zero keeps immediate retries (default).
		// Delay is full-jitter min(base*2^(n-1), 250ms). Failover hops to another
		// live node skip the wait. This is not Failover.RetryAfter (quarantine).
		RetryBackoff      time.Duration
		AllowInsecureTLS  bool
		DisableTCPNoDelay bool // false keeps TCP_NODELAY enabled
		// RequireTLS rejects New unless TLS is present. The transport default stays plaintext.
		RequireTLS bool
		// UnixSocketAnyMode accepts sockets that are other-writable (mode bit 002).
		UnixSocketAnyMode bool
		// UnixSocketMaxMode, if set, rejects sockets whose mode has bits outside the mask.
		UnixSocketMaxMode os.FileMode
	}

	// PoolEvent is one pool acquire or dial observation for optional hooks.
	PoolEvent struct {
		Server   Server
		Err      error
		Duration time.Duration
	}

	// Pool sizes the per-node connection set.
	// Zero MaxOpen/MaxIdle become 100/10. Negative MaxIdle keeps nothing idle.
	// Zero MaxLifetime, MaxIdleTime, and MinIdle mean no age limit and no warmup.
	// Idle reuse is LIFO (most recently returned first).
	Pool struct {
		MinIdle     int
		MaxIdle     int
		MaxOpen     int
		MaxLifetime time.Duration
		MaxIdleTime time.Duration
		// IdleHealthPing, when > 0, periodically sends `version` on idle pooled
		// sockets that have been unused for at least this duration.
		IdleHealthPing time.Duration
		// MaxInFlightWrites caps unacknowledged commands per connection (0 = unlimited).
		// Complements Cluster.MaxPipelineOps wave chunking; enforced on SharedStreams.
		MaxInFlightWrites int
		// SharedStreams, when > 0, enables N hot streams per node. Meta
		// mg/ms/md/ma (and Meta().Pipeline) multiplex by opaque with
		// write-while-read up to MaxInFlightWrites. Text single-ops still
		// write-coalesce with ordered reads, gated until meta in-flight is
		// idle. Session stays on pool checkout. Zero keeps classic pool checkout.
		SharedStreams int
		// OnAcquire runs only for MaxOpen slot waits (not idle hits or dials).
		// Must be non-blocking; nil disables the hook.
		OnAcquire func(PoolEvent)
		// OnCreate runs after each dial attempt from the pool (including warmup/MinIdle).
		// Must be non-blocking; nil disables the hook.
		OnCreate func(PoolEvent)
	}

	// Cluster configures key routing, bounded fanout, and optional DNS refresh.
	// Key→node routing is Rendezvous + XXH3-64.
	Cluster struct {
		// Namespace is prepended on the wire only (250-byte key limit includes it).
		// Dumps and watch streams still show the wired key.
		Namespace string
		// Selector, when non-nil, replaces built-in Rendezvous+XXH3 routing. Each Pick
		// receives the current live membership snapshot; return one of those servers.
		Selector ServerSelector
		// FanoutWorkers creates this many persistent workers and bounds concurrent
		// node operations for large clusters. Zero uses one temporary goroutine per
		// participating node for minimum latency and retains no workers.
		FanoutWorkers int
		// MaxPipelineOps caps ops per write/read wave on one connection for
		// Client.Pipeline and Meta().Pipeline (and their prepared runners).
		// Zero means unlimited. Larger batches are split so MaxOpen waiters can
		// progress between waves.
		MaxPipelineOps int
		// ResolverRefresh > 0 re-resolves ResolveSpec (or formatted Servers) on that interval.
		// AllowSetServers is required.
		ResolverRefresh time.Duration
		ResolveSpec     string // refreshed on each tick; empty uses formatted Servers
		// MemorystoreDiscovery, when true with ResolverRefresh >= 1s, treats
		// ResolveSpec (or the first Server when ResolveSpec is empty) as a
		// Memorystore config endpoint and polls `config get cluster`. New runs one
		// eager resolve before returning (seed kept + ResolverErr on failure).
		// Membership updates only when the returned ConfigID increases, and the
		// ConfigID commits only after a successful apply. AllowSetServers is required.
		MemorystoreDiscovery bool

		pickBuilder builder
		resolver    resolver
	}

	// Failover skips a node after consecutive transport errors.
	// After FailureLimit the node is ignored for RetryAfter, then DeadAfter.
	// Zero RetryAfter and DeadAfter keep the node down until a later request succeeds.
	Failover struct {
		FailureLimit int
		RetryAfter   time.Duration
		DeadAfter    time.Duration
		// Eject removes a quarantined node from live picker snapshots after
		// FailureLimit. When Eject is false, liveAddrs keeps the full membership so
		// ordinary routed ops may still hit the node; only first-live paths can
		// skip quarantined members.
		Eject bool
	}

	// Auth is text-protocol token authentication (-Y).
	Auth struct {
		User, Pass     string
		AllowPlaintext bool
	}
)
