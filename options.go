package memcached

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"time"
)

// DialContextFunc dials a memcached server. Use a tls.Dialer to speak TLS.
type DialContextFunc func(ctx context.Context, network, address string) (net.Conn, error)

// ClientOption is the option set pattern for the client.
type ClientOption func(*clientOptions)

type clientOptions struct {
	pickBuilder Builder

	// resolver is the resolver for the client to resolve the given address
	// to a list of Addr. It supports both single address and cluster address.
	// .e.g.
	//  1. single address: IP_ADDRESS:11211
	// 	2. cluster address: IP_ADDRESS:11211,IP_ADDRESS:11212,IP_ADDRESS_ADDRESS:11211
	//
	// The defaultResolver supports tcp、udp and unix domain socket. Default is tcp if the address
	// is not specified start with `udp://` or `unix://`.
	resolver Resolver

	// dialTimeout is the timeout for dialing a connection to the memcached server
	// instance. Default is 3 seconds.
	// (Connection Timeout)
	dialTimeout time.Duration
	// readTimeout is the timeout for reading from the connection.
	// Default is 5 seconds.
	// (Connection Timeout)
	readTimeout time.Duration
	// writeTimeout is the timeout for writing to the connection.
	// Default is 5 seconds.
	// (Connection Timeout)
	writeTimeout time.Duration

	// maxConns is the max connections in the pool.
	// Default is 100.
	maxConns int
	// maxIdleConns is the max idle connections in the pool.
	// Default is 10.
	maxIdleConns int
	// maxLifetime is the max lifetime for a connection, 0 means no lifetime limit.
	// Default is 0.
	maxLifetime time.Duration
	// maxIdleTimeout is the max idle timeout for a connection, 0 means no idle timeout.
	// Default is 0.
	maxIdleTimeout time.Duration

	// noReply is the flag to indicate whether the client should wait for the response.
	noReply bool

	// allowFlushAll enables the destructive FlushAll broadcast.
	allowFlushAll bool
	// allowAdmin enables Admin() / AdminClient.
	allowAdmin bool

	dialContext DialContextFunc

	tlsConfig *tls.Config
	tlsSet    bool

	authUser string
	authPass string

	retryAttempts int

	failover FailoverConfig

	keepAlive       time.Duration
	minIdleConns    int
	namespace       string
	hashWithPrefix  bool
	resolverRefresh time.Duration
}

// FailoverConfig marks nodes dead after consecutive transport failures.
// EjectFromHash + crc32/modulo remaps every key; prefer later Ketama/Rendezvous.
type FailoverConfig struct {
	FailureLimit  int
	RetryTimeout  time.Duration
	EjectFromHash bool
}

func newClientOptions() *clientOptions {
	return &clientOptions{
		pickBuilder: crc32HashPickBuilder{},
		resolver:    defaultResolver{},

		dialTimeout:  3 * time.Second,
		readTimeout:  5 * time.Second,
		writeTimeout: 5 * time.Second,

		maxConns:       100,
		maxIdleConns:   10,
		maxLifetime:    0,
		maxIdleTimeout: 0,

		noReply:       false,
		retryAttempts: 1,
	}
}

// WithResolver sets the resolver for the client to resolve the given address
// to a list of Addr.
func WithResolver(r Resolver) ClientOption {
	return func(o *clientOptions) {
		if r == nil {
			return
		}

		o.resolver = r
	}
}

// WithPickBuilder sets the pickBuilder for the client to build a Picker from
// a list of Addr.
func WithPickBuilder(p Builder) ClientOption {
	return func(o *clientOptions) {
		if p == nil {
			return
		}

		o.pickBuilder = p
	}
}

// WithDialTimeout sets the dial timeout for the client.
// Default is 3 seconds.
func WithDialTimeout(timeout time.Duration) ClientOption {
	return func(o *clientOptions) {
		if timeout <= 0 {
			timeout = 3 * time.Second
		}

		o.dialTimeout = timeout
	}
}

// WithReadTimeout sets the read timeout for the client.
// Default is 5 seconds.
func WithReadTimeout(timeout time.Duration) ClientOption {
	return func(o *clientOptions) {
		if timeout <= 0 {
			timeout = 5 * time.Second
		}

		o.readTimeout = timeout
	}
}

// WithWriteTimeout sets the write timeout for the client.
// Default is 5 seconds.
func WithWriteTimeout(timeout time.Duration) ClientOption {
	return func(o *clientOptions) {
		if timeout <= 0 {
			timeout = 5 * time.Second
		}

		o.writeTimeout = timeout
	}
}

// WithMaxConns sets the max connections in the pool.
func WithMaxConns(n int) ClientOption {
	return func(o *clientOptions) {
		if n <= 0 {
			n = 100
		}

		o.maxConns = n
	}
}

// WithMaxIdleConns sets the max idle connections in the pool.
func WithMaxIdleConns(n int) ClientOption {
	return func(o *clientOptions) {
		if n <= 0 {
			n = 10
		}

		o.maxIdleConns = n
	}
}

// WithMaxLifetime sets the max lifetime for a connection, 0 means no lifetime limit.
func WithMaxLifetime(d time.Duration) ClientOption {
	return func(o *clientOptions) {
		o.maxLifetime = d
	}
}

// WithMaxIdleTimeout sets the max idle timeout for a connection, 0 means no idle timeout.
func WithMaxIdleTimeout(d time.Duration) ClientOption {
	return func(o *clientOptions) {
		o.maxIdleTimeout = d
	}
}

// WithNoReply sets the flag to indicate whether the client should wait for the response.
func WithNoReply() ClientOption {
	return func(o *clientOptions) {
		o.noReply = true
	}
}

// WithUDPEnabled is a no-op. UDP framing is used only for addresses whose
// network is udp, udp4, or udp6 (for example udp://127.0.0.1:11211).
func WithUDPEnabled() ClientOption {
	return func(*clientOptions) {}
}

// WithAllowFlushAll permits FlushAll, which invalidates every item on every
// configured server. Without this option FlushAll returns ErrFlushAllNotAllowed.
func WithAllowFlushAll() ClientOption {
	return func(o *clientOptions) {
		o.allowFlushAll = true
	}
}

// WithAdminCommands permits Client.Admin(). Admin ops can shut down a server,
// move memory, or dump keys. They are not available on the default Client.
func WithAdminCommands() ClientOption {
	return func(o *clientOptions) {
		o.allowAdmin = true
	}
}

// WithDialContext sets a custom dialer. The request context is additionally
// bounded by DialTimeout so a custom dialer cannot hang forever.
func WithDialContext(fn DialContextFunc) ClientOption {
	return func(o *clientOptions) {
		if fn == nil {
			return
		}
		o.dialContext = fn
	}
}

// WithTLS wraps the default dialer in TLS. cfg must be non-nil.
// Per connection the config is cloned and ServerName is filled from the
// host:port address when empty (Unix sockets are left unchanged).
// WithDialContext, if set, remains the dialer and WithTLS is not applied.
// Do not set InsecureSkipVerify except for tests.
func WithTLS(cfg *tls.Config) ClientOption {
	return func(o *clientOptions) {
		o.tlsSet = true
		o.tlsConfig = cfg
	}
}

func tlsConfigForDial(cfg *tls.Config, network, address string) *tls.Config {
	c := cfg.Clone()
	if c.ServerName == "" && !strings.HasPrefix(network, "unix") {
		if host, _, err := net.SplitHostPort(address); err == nil {
			c.ServerName = host
		}
	}
	return c
}

func tlsDialContext(cfg *tls.Config, keepAlive time.Duration) DialContextFunc {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		d := &net.Dialer{KeepAlive: keepAlive}
		raw, err := d.DialContext(ctx, network, address)
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

func applyTLSDialer(o *clientOptions) error {
	if !o.tlsSet {
		return nil
	}
	if o.tlsConfig == nil {
		return fmt.Errorf("WithTLS: nil config: %w", ErrInvalidArgument)
	}
	if o.dialContext == nil {
		o.dialContext = tlsDialContext(o.tlsConfig, o.keepAlive)
	}
	return nil
}

// WithAuth sends memcached token authentication (-Y) after each new dial,
// before the connection is pooled. Use only with TLS or Unix/loopback.
func WithAuth(user, pass string) ClientOption {
	return func(o *clientOptions) {
		o.authUser = user
		o.authPass = pass
	}
}

// WithRetry retries connect/transport errors. attempts=2 means one retry.
// Protocol errors such as ErrNotFound are never retried.
func WithRetry(attempts int) ClientOption {
	return func(o *clientOptions) {
		if attempts < 1 {
			attempts = 1
		}
		o.retryAttempts = attempts
	}
}

// WithFailover ejects or quarantines nodes after FailureLimit transport errors.
func WithFailover(cfg FailoverConfig) ClientOption {
	return func(o *clientOptions) {
		if cfg.FailureLimit <= 0 {
			cfg.FailureLimit = 1
		}
		if cfg.RetryTimeout <= 0 {
			cfg.RetryTimeout = 30 * time.Second
		}
		o.failover = cfg
	}
}

// WithKeepAlive sets TCP keepalive idle time on the default dialer.
// A custom WithDialContext is unchanged (documented no-op).
func WithKeepAlive(idle time.Duration) ClientOption {
	return func(o *clientOptions) {
		o.keepAlive = idle
	}
}

// WithMinIdleConns warms this many idle connections per node (capped by MaxIdle).
func WithMinIdleConns(n int) ClientOption {
	return func(o *clientOptions) {
		if n < 0 {
			n = 0
		}
		o.minIdleConns = n
	}
}

// WithNamespace prefixes keys on the wire. The 250-byte key limit applies after
// the prefix. If hashWithPrefix is true the picker sees the prefixed key.
func WithNamespace(prefix string, hashWithPrefix bool) ClientOption {
	return func(o *clientOptions) {
		o.namespace = prefix
		o.hashWithPrefix = hashWithPrefix
	}
}

// WithResolverRefresh periodically re-resolves the original address string
// and applies SetServers. Close stops the ticker.
func WithResolverRefresh(every time.Duration) ClientOption {
	return func(o *clientOptions) {
		o.resolverRefresh = every
	}
}
