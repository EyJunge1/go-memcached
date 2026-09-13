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

type clientOptions struct {
	pickBuilder Builder
	resolver    Resolver

	dialTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration

	maxConns       int
	maxIdleConns   int
	maxLifetime    time.Duration
	maxIdleTimeout time.Duration

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
	minIdleConns    int
	namespace       string
	hashWithPrefix  bool
	resolverRefresh time.Duration
	maxValueBytes   int

	tcpNoDelay        bool
	unixSocketAnyMode bool
	unixSocketMaxMode os.FileMode
}

func optionsFromConfig(cfg Config) (*clientOptions, error) {
	o := &clientOptions{
		pickBuilder: cfg.Cluster.PickBuilder,
		resolver:    cfg.Cluster.Resolver,

		dialTimeout:  cfg.Transport.DialTimeout,
		readTimeout:  cfg.Transport.ReadTimeout,
		writeTimeout: cfg.Transport.WriteTimeout,

		maxConns:       cfg.Pool.MaxOpen,
		maxIdleConns:   cfg.Pool.MaxIdle,
		maxLifetime:    cfg.Pool.MaxLifetime,
		maxIdleTimeout: cfg.Pool.MaxIdleTime,
		minIdleConns:   cfg.Pool.MinIdle,

		allowFlushAll:   cfg.AllowFlushAll,
		allowAdmin:      cfg.AllowAdmin,
		allowSetServers: cfg.AllowSetServers,

		dialContext:   cfg.Transport.Dial,
		keepAlive:     cfg.Transport.KeepAlive,
		retryAttempts: cfg.Transport.RetryAttempts,

		namespace:         cfg.Cluster.Namespace,
		hashWithPrefix:    cfg.Cluster.HashWithPrefix,
		resolverRefresh:   cfg.Cluster.ResolverRefresh,
		maxValueBytes:     cfg.MaxValueBytes,
		tcpNoDelay:        tcpNoDelayEnabled(cfg.Transport.TCPNoDelay),
		unixSocketAnyMode: cfg.Transport.UnixSocketAnyMode,
		unixSocketMaxMode: cfg.Transport.UnixSocketMaxMode,
	}
	if o.maxValueBytes <= 0 {
		o.maxValueBytes = defaultMaxValueBytes
	}
	if o.pickBuilder == nil {
		o.pickBuilder = defaultPickBuilder()
	}
	if o.resolver == nil {
		o.resolver = defaultResolver{}
	}
	if isZeroTransport(cfg.Transport) {
		o.dialTimeout = 3 * time.Second
		o.readTimeout = 5 * time.Second
		o.writeTimeout = 5 * time.Second
		o.retryAttempts = 1
	}
	if isZeroPool(cfg.Pool) {
		o.maxConns = 100
		o.maxIdleConns = 10
	} else if o.maxConns <= 0 {
		return nil, fmt.Errorf("Pool.MaxOpen must be > 0: %w", ErrInvalidArgument)
	}
	if o.minIdleConns < 0 {
		o.minIdleConns = 0
	}
	if cfg.Transport.TLS != nil {
		o.tlsSet = true
		o.tlsConfig = cfg.Transport.TLS
	}
	if cfg.Auth != nil {
		o.authUser = cfg.Auth.User
		o.authPass = cfg.Auth.Pass
		o.authAllowPlain = cfg.Auth.AllowPlaintext
	}
	o.failover = applyFailover(cfg, len(cfg.Servers))
	if err := applyTLSDialer(o); err != nil {
		return nil, err
	}
	return o, nil
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

func applyTLSDialer(o *clientOptions) error {
	if !o.tlsSet {
		return nil
	}
	if o.tlsConfig == nil {
		return fmt.Errorf("TLS: nil config: %w", ErrInvalidArgument)
	}
	inner := o.dialContext
	if inner == nil {
		inner = defaultRawDial(o.keepAlive)
	}
	o.dialContext = wrapTLSDial(inner, o.tlsConfig)
	return nil
}
