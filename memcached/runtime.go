package memcached

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

// DialContextFunc opens the raw socket. A non-nil Transport.TLS wraps it with tls.Client.
type DialContextFunc func(ctx context.Context, network, address string) (net.Conn, error)

// runtime is Config after New: defaults filled in and the TLS dialer applied.
type runtime struct {
	pickBuilder builder
	resolver    resolver

	dialTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration

	maxOpen           int
	maxIdle           int
	maxLifetime       time.Duration
	maxIdleTime       time.Duration
	idleHealthPing    time.Duration
	maxInFlightWrites int
	sharedStreams     int // 0 = off; N write-coalescing hot conns per node
	onAcquire         func(PoolEvent)
	onCreate          func(PoolEvent)
	observer          Observer

	allowFlushAll   bool
	allowAdmin      bool
	allowSetServers bool

	dialContext DialContextFunc
	customDial  bool

	tlsConfig        *tls.Config
	tlsSet           bool
	allowInsecureTLS bool
	udp              bool

	authUser             string
	authPass             string
	authAllowPlain       bool
	retryAttempts        int
	retryBackoff         time.Duration
	failover             Failover
	keepAlive            time.Duration
	minIdle              int
	namespace            string
	nsPrefix             []byte
	resolverRefresh      time.Duration
	maxValueBytes        int
	fanoutWorkers        int
	maxPipelineOps       int
	memorystoreDiscovery bool

	tcpNoDelay        bool
	unixSocketAnyMode bool
	unixSocketMaxMode os.FileMode
}

// runtimeFromConfig builds a runtime from Config, filling defaults and applying the TLS dialer.
func runtimeFromConfig(cfg Config) (*runtime, error) {
	rt := &runtime{
		pickBuilder: cfg.Cluster.pickBuilder,
		resolver:    cfg.Cluster.resolver,

		dialTimeout:  cfg.Transport.DialTimeout,
		readTimeout:  cfg.Transport.ReadTimeout,
		writeTimeout: cfg.Transport.WriteTimeout,

		maxOpen:           cfg.Pool.MaxOpen,
		maxIdle:           cfg.Pool.MaxIdle,
		maxLifetime:       cfg.Pool.MaxLifetime,
		maxIdleTime:       cfg.Pool.MaxIdleTime,
		minIdle:           cfg.Pool.MinIdle,
		idleHealthPing:    cfg.Pool.IdleHealthPing,
		maxInFlightWrites: cfg.Pool.MaxInFlightWrites,
		sharedStreams:     cfg.Pool.SharedStreams,
		onAcquire:         cfg.Pool.OnAcquire,
		onCreate:          cfg.Pool.OnCreate,
		observer:          cfg.Observer,

		allowFlushAll:   cfg.AllowFlushAll,
		allowAdmin:      cfg.AllowAdmin,
		allowSetServers: cfg.AllowSetServers,

		dialContext:   cfg.Transport.Dial,
		customDial:    cfg.Transport.Dial != nil,
		keepAlive:     cfg.Transport.KeepAlive,
		retryAttempts: cfg.Transport.RetryAttempts,
		retryBackoff:  cfg.Transport.RetryBackoff,

		namespace:            cfg.Cluster.Namespace,
		resolverRefresh:      cfg.Cluster.ResolverRefresh,
		maxValueBytes:        cfg.MaxValueBytes,
		fanoutWorkers:        cfg.Cluster.FanoutWorkers,
		maxPipelineOps:       cfg.Cluster.MaxPipelineOps,
		memorystoreDiscovery: cfg.Cluster.MemorystoreDiscovery,
		tcpNoDelay:           !cfg.Transport.DisableTCPNoDelay,
		unixSocketAnyMode:    cfg.Transport.UnixSocketAnyMode,
		unixSocketMaxMode:    cfg.Transport.UnixSocketMaxMode,
	}
	if rt.maxValueBytes <= 0 {
		rt.maxValueBytes = _defaultMaxValueBytes
	}
	if rt.namespace != "" {
		rt.nsPrefix = []byte(rt.namespace)
	}
	if rt.pickBuilder == nil {
		rt.pickBuilder = defaultPickBuilder()
		if cfg.Cluster.Selector != nil {
			rt.pickBuilder = selectorBuilder{sel: cfg.Cluster.Selector}
		}
	}
	rt.dialTimeout = timeoutOrDefault(cfg.Transport.DialTimeout, 3*time.Second)
	rt.readTimeout = timeoutOrDefault(cfg.Transport.ReadTimeout, 5*time.Second)
	rt.writeTimeout = timeoutOrDefault(cfg.Transport.WriteTimeout, 5*time.Second)
	if rt.retryAttempts == 0 {
		rt.retryAttempts = 1
	}
	if rt.maxOpen == 0 {
		rt.maxOpen = 100
	}
	if cfg.Pool.MaxIdle < 0 {
		rt.maxIdle = 0
	} else if rt.maxIdle == 0 {
		rt.maxIdle = 10
	}
	if rt.minIdle < 0 {
		rt.minIdle = 0
	}
	rt.allowInsecureTLS = cfg.Transport.AllowInsecureTLS
	if cfg.Transport.TLS != nil {
		rt.tlsSet = true
		rt.tlsConfig = cfg.Transport.TLS.Clone()
	}
	rt.udp = serversAreUDP(cfg.Servers)
	if cfg.Auth != nil {
		rt.authUser = cfg.Auth.User
		rt.authPass = cfg.Auth.Pass
		rt.authAllowPlain = cfg.Auth.AllowPlaintext
	}
	rt.failover = cfg.Failover
	if err := applyTLSDialer(rt); err != nil {
		return nil, err
	}
	if rt.resolver == nil {
		var resolver defaultResolver
		rt.resolver = resolver
		if rt.memorystoreDiscovery {
			endpoint := cfg.Cluster.ResolveSpec
			if endpoint == "" {
				endpoint = formatServerSpec(cfg.Servers[:1])
			}
			rt.resolver = newMemorystoreResolver(endpoint, rt)
		}
	}
	return rt, nil
}

// timeoutOrDefault returns v, or def when v is 0, or 0 when v is TimeoutNone.
func timeoutOrDefault(v, def time.Duration) time.Duration {
	if v == 0 {
		return def
	}
	if v == TimeoutNone {
		return 0
	}
	return v
}

// tlsConfigForDial clones cfg, requires TLS 1.2+, and fills ServerName from a TCP address when empty.
func tlsConfigForDial(cfg *tls.Config, network, address string) *tls.Config {
	if cfg == nil {
		cfg = &tls.Config{}
	}
	c := cfg.Clone()
	if c == nil {
		c = &tls.Config{}
	}
	if c.MinVersion == 0 {
		c.MinVersion = tls.VersionTLS12
	}
	if c.ServerName == "" && !strings.HasPrefix(network, "unix") {
		if host, _, err := net.SplitHostPort(address); err == nil {
			c.ServerName = host
		}
	}
	return c
}

// wrapTLSDial returns a dialer that TLS-wraps inner and completes the handshake.
func wrapTLSDial(inner DialContextFunc, cfg *tls.Config, allowInsecure bool) DialContextFunc {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		raw, err := inner(ctx, network, address)
		if err != nil {
			return nil, err
		}
		closeRaw := true
		defer func() {
			if closeRaw {
				_ = raw.Close()
			}
		}()
		tcfg := tlsConfigForDial(cfg, network, address)
		if tcfg.InsecureSkipVerify && !allowInsecure {
			return nil, ErrInsecureTLS
		}
		tconn := tls.Client(raw, tcfg)
		if err := tconn.HandshakeContext(ctx); err != nil {
			return nil, err
		}
		closeRaw = false
		return tconn, nil
	}
}

// defaultRawDial returns a DialContextFunc that opens a raw socket with the given keep-alive.
func defaultRawDial(keepAlive time.Duration) DialContextFunc {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		d := &net.Dialer{KeepAlive: keepAlive}
		return d.DialContext(ctx, network, address)
	}
}

// applyTLSDialer wraps runtime.dialContext with TLS when Transport.TLS was set.
func applyTLSDialer(rt *runtime) error {
	if !rt.tlsSet {
		return nil
	}
	if rt.tlsConfig == nil {
		return fmt.Errorf("tls: nil config: %w", ErrInvalidArgument)
	}
	inner := rt.dialContext
	if inner == nil {
		inner = defaultRawDial(rt.keepAlive)
	}
	rt.dialContext = wrapTLSDial(inner, rt.tlsConfig, rt.allowInsecureTLS)
	return nil
}
