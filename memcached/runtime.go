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

// DialContextFunc dials a raw socket. If Transport.TLS is set, the client
// always wraps that socket with tls.Client and handshakes.
type DialContextFunc func(ctx context.Context, network, address string) (net.Conn, error)

// runtime is the resolved Config after New: defaults applied, TLS dialer wrapped.
type runtime struct {
	pickBuilder builder
	resolver    resolver

	dialTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration

	maxOpen     int
	maxIdle     int
	maxLifetime time.Duration
	maxIdleTime time.Duration

	allowFlushAll   bool
	allowAdmin      bool
	allowSetServers bool

	dialContext DialContextFunc

	tlsConfig *tls.Config
	tlsSet    bool

	authUser        string
	authPass        string
	authAllowPlain  bool
	retryAttempts   int
	failover        Failover
	keepAlive       time.Duration
	minIdle         int
	namespace       string
	resolverRefresh time.Duration
	maxValueBytes   int

	tcpNoDelay        bool
	unixSocketAnyMode bool
	unixSocketMaxMode os.FileMode
}

func runtimeFromConfig(cfg Config) (*runtime, error) {
	rt := &runtime{
		pickBuilder: cfg.Cluster.pickBuilder,
		resolver:    cfg.Cluster.resolver,

		dialTimeout:  cfg.Transport.DialTimeout,
		readTimeout:  cfg.Transport.ReadTimeout,
		writeTimeout: cfg.Transport.WriteTimeout,

		maxOpen:     cfg.Pool.MaxOpen,
		maxIdle:     cfg.Pool.MaxIdle,
		maxLifetime: cfg.Pool.MaxLifetime,
		maxIdleTime: cfg.Pool.MaxIdleTime,
		minIdle:     cfg.Pool.MinIdle,

		allowFlushAll:   cfg.AllowFlushAll,
		allowAdmin:      cfg.AllowAdmin,
		allowSetServers: cfg.AllowSetServers,

		dialContext:   cfg.Transport.Dial,
		keepAlive:     cfg.Transport.KeepAlive,
		retryAttempts: cfg.Transport.RetryAttempts,

		namespace:         cfg.Cluster.Namespace,
		resolverRefresh:   cfg.Cluster.ResolverRefresh,
		maxValueBytes:     cfg.MaxValueBytes,
		tcpNoDelay:        !cfg.Transport.DisableTCPNoDelay,
		unixSocketAnyMode: cfg.Transport.UnixSocketAnyMode,
		unixSocketMaxMode: cfg.Transport.UnixSocketMaxMode,
	}
	if rt.maxValueBytes <= 0 {
		rt.maxValueBytes = defaultMaxValueBytes
	}
	if rt.pickBuilder == nil {
		rt.pickBuilder = defaultPickBuilder()
	}
	if rt.resolver == nil {
		rt.resolver = defaultResolver{}
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
	if cfg.Transport.TLS != nil {
		rt.tlsSet = true
		rt.tlsConfig = cfg.Transport.TLS
	}
	if cfg.Auth != nil {
		rt.authUser = cfg.Auth.User
		rt.authPass = cfg.Auth.Pass
		rt.authAllowPlain = cfg.Auth.AllowPlaintext
	}
	rt.failover = cfg.Failover
	if err := applyTLSDialer(rt); err != nil {
		return nil, err
	}
	return rt, nil
}

func timeoutOrDefault(v, def time.Duration) time.Duration {
	if v == 0 {
		return def
	}
	if v == TimeoutNone {
		return 0
	}
	return v
}

func tlsConfigForDial(cfg *tls.Config, network, address string) *tls.Config {
	c := cfg.Clone()
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

func wrapTLSDial(inner DialContextFunc, cfg *tls.Config) DialContextFunc {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		raw, err := inner(ctx, network, address)
		if err != nil {
			return nil, err
		}
		tconn := tls.Client(raw, tlsConfigForDial(cfg, network, address))
		if err = tconn.HandshakeContext(ctx); err != nil {
			_ = raw.Close()
			return nil, err
		}
		return tconn, nil
	}
}

func defaultRawDial(keepAlive time.Duration) DialContextFunc {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		d := &net.Dialer{KeepAlive: keepAlive}
		return d.DialContext(ctx, network, address)
	}
}

func applyTLSDialer(rt *runtime) error {
	if !rt.tlsSet {
		return nil
	}
	if rt.tlsConfig == nil {
		return fmt.Errorf("TLS: nil config: %w", ErrInvalidArgument)
	}
	inner := rt.dialContext
	if inner == nil {
		inner = defaultRawDial(rt.keepAlive)
	}
	rt.dialContext = wrapTLSDial(inner, rt.tlsConfig)
	return nil
}
