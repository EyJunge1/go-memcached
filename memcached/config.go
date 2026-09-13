package memcached

import (
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// TimeoutNone disables a Transport timeout (no extra deadline beyond ctx).
// A zero DialTimeout / ReadTimeout / WriteTimeout still means the package default.
const TimeoutNone time.Duration = -1

// Config is the only constructor input for New.
// Optional fields follow one rule for the whole API:
//
//	A — opt-in blocks (Failover, Auth): zero or nil means off.
//	B — default-on: inverted bool (DisableTCPNoDelay), never *bool.
//	C — 0 is a valid wire value: pointer (GetOpts.Touch, FlushOpts.Delay, Meta *Exptime / *uint64).
//	D — 0 is not useful: omit on zero (MetaGet.Recache, Cluster.ResolverRefresh).
//	E — scalar defaults apply field-wise (DialTimeout 0 → 3s; TimeoutNone = unlimited).
type Config struct {
	Servers         []Server
	Transport       Transport
	Pool            Pool
	Cluster         Cluster
	Failover        Failover // zero = off; use DefaultFailover() for the previous multi-node default
	Auth            *Auth
	AllowFlushAll   bool // required for Admin On/All Flush; zero denies flush_all
	AllowAdmin      bool // required for Client.Admin
	AllowSetServers bool // required for SetServers and Cluster.ResolverRefresh
	MaxValueBytes   int  // 0 → 1 MiB; applies to write validate and VALUE/VA reads
}

// Server is one cluster node.
type Server struct {
	Network string // "tcp" (default), "unix", "udp"
	Address string
	Weight  uint32 // 0 ⇒ 1; used by Ketama
}

// Transport holds dial, TLS, and I/O timeouts.
// Defaults apply field-wise: DialTimeout 0 → 3s, ReadTimeout/WriteTimeout 0 → 5s,
// RetryAttempts 0 → 1. TimeoutNone on a timeout field means no extra deadline.
// Dial is the raw socket; TLS, if set, always wraps it.
type Transport struct {
	TLS               *tls.Config
	Dial              DialContextFunc
	DialTimeout       time.Duration // 0 → 3s; TimeoutNone = no extra timeout
	ReadTimeout       time.Duration // 0 → 5s; TimeoutNone = no extra timeout
	WriteTimeout      time.Duration // 0 → 5s; TimeoutNone = no extra timeout
	KeepAlive         time.Duration // 0 → net.Dialer default
	RetryAttempts     int           // 0 → 1
	AllowInsecureTLS  bool
	DisableTCPNoDelay bool // zero = TCP_NODELAY on
	// RequireTLS makes New fail unless TLS is configured. Default remains plaintext.
	RequireTLS bool
	// UnixSocketAnyMode skips the default reject of other-write (002).
	UnixSocketAnyMode bool
	// UnixSocketMaxMode, when non-zero, rejects sockets with extra permission bits.
	UnixSocketMaxMode os.FileMode
}

// Pool is the per-node connection pool.
// Defaults apply field-wise: MaxOpen 0 → 100, MaxIdle 0 → 10. MaxIdle < 0 keeps
// no idle connections. MaxLifetime/MaxIdleTime 0 stay unlimited. MinIdle 0
// means no warmup.
type Pool struct {
	MinIdle     int
	MaxIdle     int
	MaxOpen     int
	MaxLifetime time.Duration
	MaxIdleTime time.Duration
}

// Cluster selects namespace and optional resolver refresh.
// The picker is always weighted Ketama/MD5.
type Cluster struct {
	Namespace string
	// ResolverRefresh, when > 0, re-resolves ResolveSpec (or formatted Servers)
	// on that interval. Requires AllowSetServers.
	ResolverRefresh time.Duration
	ResolveSpec     string // re-resolved on refresh; empty → formatted Servers

	pickBuilder builder
	resolver    resolver
}

// Failover quarantines nodes after consecutive transport failures.
// After FailureLimit, the node is skipped for RetryAfter; further failures
// use DeadAfter. Zero durations mean no timed revive (until a success).
type Failover struct {
	FailureLimit int
	RetryAfter   time.Duration
	DeadAfter    time.Duration
	Eject        bool
}

// Auth is memcached text token authentication (-Y).
type Auth struct {
	User, Pass     string
	AllowPlaintext bool
}

// ParseServers parses a comma-separated spec (tcp host:port, unix://, udp://, ?weight=).
func ParseServers(spec string) ([]Server, error) {
	addrs, err := defaultResolver{}.Resolve(spec)
	if err != nil {
		return nil, err
	}
	return addrsToServers(addrs), nil
}

func addrsToServers(addrs []*addr) []Server {
	out := make([]Server, 0, len(addrs))
	for _, a := range addrs {
		if a == nil {
			continue
		}
		out = append(out, addrToServer(a))
	}
	return out
}

func addrToServer(a *addr) Server {
	return Server{Network: a.Network, Address: a.Address, Weight: a.Weight}
}

func serversToAddrs(servers []Server) ([]*addr, error) {
	if len(servers) == 0 {
		return nil, fmt.Errorf("empty address: %w", ErrInvalidAddress)
	}
	out := make([]*addr, 0, len(servers))
	for _, s := range servers {
		network := s.Network
		if network == "" {
			network = "tcp"
		}
		switch network {
		case "tcp", "tcp4", "tcp6", "unix", "udp", "udp4", "udp6":
		default:
			return nil, fmt.Errorf("invalid network %q: %w", network, ErrInvalidNetworkProtocol)
		}
		if strings.TrimSpace(s.Address) == "" {
			return nil, fmt.Errorf("empty address: %w", ErrInvalidAddress)
		}
		out = append(out, newAddrWeighted(network, s.Address, s.Weight))
	}
	return out, nil
}

func formatServerSpec(servers []Server) string {
	parts := make([]string, 0, len(servers))
	for _, s := range servers {
		p := s.Address
		switch s.Network {
		case "unix":
			p = "unix://" + s.Address
		case "udp", "udp4", "udp6":
			p = "udp://" + s.Address
		}
		if s.Weight > 0 {
			p += "?weight=" + strconv.FormatUint(uint64(s.Weight), 10)
		}
		parts = append(parts, p)
	}
	return strings.Join(parts, ",")
}

func serverNetwork(s Server) string {
	if s.Network == "" {
		return "tcp"
	}
	return s.Network
}

func isLoopbackServer(s Server) bool {
	if strings.HasPrefix(serverNetwork(s), "unix") {
		return true
	}
	host := s.Address
	if h, _, err := net.SplitHostPort(s.Address); err == nil {
		host = h
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func isStreamNetwork(network string) bool {
	switch network {
	case "", "tcp", "tcp4", "tcp6", "unix":
		return true
	default:
		return false
	}
}

func validateTransportServers(servers []Server, tlsCfg *tls.Config) error {
	var sawUDP, sawStream bool
	for _, s := range servers {
		if isUDPNetwork(serverNetwork(s)) {
			sawUDP = true
		} else if isStreamNetwork(serverNetwork(s)) {
			sawStream = true
		}
	}
	if sawUDP && sawStream {
		return fmt.Errorf("udp cannot be mixed with tcp/unix: %w", ErrMixedTransport)
	}
	if tlsCfg != nil && sawUDP {
		return fmt.Errorf("tls is not supported on UDP: %w", ErrNotSupported)
	}
	return nil
}

func validateAuthAgainstServers(servers []Server, allowPlain, hasTLS bool) error {
	hasUDP := false
	needsPlaintextGuard := !hasTLS && !allowPlain
	for _, s := range servers {
		if isUDPNetwork(serverNetwork(s)) {
			hasUDP = true
		}
		if needsPlaintextGuard && !isLoopbackServer(s) {
			return fmt.Errorf("token auth requires TLS, unix, loopback, or Auth.AllowPlaintext: %w", ErrInvalidArgument)
		}
	}
	if hasUDP {
		return fmt.Errorf("token auth is not supported on UDP: %w", ErrNotSupported)
	}
	return nil
}

func validateConfig(cfg Config) error {
	if len(cfg.Servers) == 0 {
		return fmt.Errorf("empty address: %w", ErrInvalidAddress)
	}
	if cfg.Cluster.ResolverRefresh > 0 && !cfg.AllowSetServers {
		return fmt.Errorf("ResolverRefresh requires AllowSetServers: %w", ErrSetServersNotAllowed)
	}
	if cfg.Pool.MaxOpen < 0 {
		return fmt.Errorf("Pool.MaxOpen must be >= 0: %w", ErrInvalidArgument)
	}
	if err := validateTransportServers(cfg.Servers, cfg.Transport.TLS); err != nil {
		return err
	}
	if cfg.Transport.RequireTLS && cfg.Transport.TLS == nil {
		return fmt.Errorf("RequireTLS requires Transport.TLS: %w", ErrInvalidArgument)
	}
	if cfg.Transport.TLS != nil && cfg.Transport.TLS.InsecureSkipVerify && !cfg.Transport.AllowInsecureTLS {
		return fmt.Errorf("%w", ErrInsecureTLS)
	}
	if cfg.Auth != nil {
		if cfg.Auth.User == "" {
			return fmt.Errorf("auth user empty: %w", ErrInvalidArgument)
		}
		if err := validAuthSecret(cfg.Auth.User); err != nil {
			return err
		}
		if err := validAuthSecret(cfg.Auth.Pass); err != nil {
			return err
		}
		if err := validateAuthAgainstServers(cfg.Servers, cfg.Auth.AllowPlaintext, cfg.Transport.TLS != nil); err != nil {
			return err
		}
	}
	return nil
}

// DefaultFailover is the previous multi-node default: two failures, 1s retry, 30s dead, eject.
func DefaultFailover() Failover {
	return Failover{
		FailureLimit: 2,
		RetryAfter:   time.Second,
		DeadAfter:    30 * time.Second,
		Eject:        true,
	}
}
