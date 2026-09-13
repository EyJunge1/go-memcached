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

// Config is the only constructor input for New.
type Config struct {
	Servers         []Server
	Transport       Transport
	Pool            Pool
	Cluster         Cluster
	Failover        *Failover // nil = on when len(Servers)>1; &Failover{} = off
	Auth            *Auth
	AllowFlushAll   bool
	AllowAdmin      bool
	AllowSetServers bool
	MaxValueBytes   int // 0 → 1 MiB; applies to write validate and VALUE/VA reads
}

// Server is one cluster node.
type Server struct {
	Network string // "tcp" (default), "unix", "udp"
	Address string
	Weight  uint32 // 0 ⇒ 1; used by Ketama
}

// Transport holds dial, TLS, and I/O timeouts.
// The zero value applies package defaults (3s/5s/5s, RetryAttempts=1).
// Dial is the raw socket; TLS, if set, always wraps it.
type Transport struct {
	TLS              *tls.Config
	Dial             DialContextFunc
	DialTimeout      time.Duration
	ReadTimeout      time.Duration
	WriteTimeout     time.Duration
	KeepAlive        time.Duration
	RetryAttempts    int
	AllowInsecureTLS bool
	// TCPNoDelay defaults to true when nil.
	TCPNoDelay *bool
	// RequireTLS makes New fail unless TLS is configured. Default remains plaintext.
	RequireTLS bool
	// UnixSocketAnyMode skips the default reject of other-write (002).
	UnixSocketAnyMode bool
	// UnixSocketMaxMode, when non-zero, rejects sockets with extra permission bits.
	UnixSocketMaxMode os.FileMode
}

// Pool is the per-node connection pool.
// The zero value applies MaxOpen=100, MaxIdle=10. MaxLifetime/MaxIdleTime 0
// stay unlimited. A non-zero Pool with MaxOpen==0 is invalid; MaxIdle==0
// keeps no idle connections.
type Pool struct {
	MinIdle     int
	MaxIdle     int
	MaxOpen     int
	MaxLifetime time.Duration
	MaxIdleTime time.Duration
}

// Cluster selects hashing, namespace, and optional resolver refresh.
type Cluster struct {
	PickBuilder     Builder // nil → weighted Ketama/MD5
	Namespace       string
	HashWithPrefix  bool
	Resolver        Resolver
	ResolverRefresh time.Duration
	ResolveSpec     string // re-resolved on refresh; empty → formatted Servers
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

func addrsToServers(addrs []*Addr) []Server {
	out := make([]Server, 0, len(addrs))
	for _, a := range addrs {
		if a == nil {
			continue
		}
		out = append(out, addrToServer(a))
	}
	return out
}

func addrToServer(a *Addr) Server {
	return Server{Network: a.Network, Address: a.Address, Weight: a.Weight}
}

func serversToAddrs(servers []Server) ([]*Addr, error) {
	if len(servers) == 0 {
		return nil, fmt.Errorf("empty address: %w", ErrInvalidAddress)
	}
	out := make([]*Addr, 0, len(servers))
	for i, s := range servers {
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
		out = append(out, NewAddrWeighted(network, s.Address, i, s.Weight))
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

func isZeroTransport(t Transport) bool {
	return t.TLS == nil && t.Dial == nil && t.DialTimeout == 0 && t.ReadTimeout == 0 &&
		t.WriteTimeout == 0 && t.KeepAlive == 0 && t.RetryAttempts == 0 && !t.AllowInsecureTLS &&
		t.TCPNoDelay == nil && !t.RequireTLS && !t.UnixSocketAnyMode && t.UnixSocketMaxMode == 0
}

func tcpNoDelayEnabled(p *bool) bool {
	return p == nil || *p
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

func isZeroPool(p Pool) bool {
	return p.MinIdle == 0 && p.MaxIdle == 0 && p.MaxOpen == 0 &&
		p.MaxLifetime == 0 && p.MaxIdleTime == 0
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
	if cfg.Pool.MaxIdle < 0 {
		return fmt.Errorf("Pool.MaxIdle must be >= 0: %w", ErrInvalidArgument)
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

func applyFailover(cfg Config, n int) Failover {
	if n <= 1 {
		return Failover{}
	}
	if cfg.Failover == nil {
		return Failover{
			FailureLimit: 2,
			RetryAfter:   time.Second,
			DeadAfter:    30 * time.Second,
			Eject:        true,
		}
	}
	return *cfg.Failover
}

func defaultPickBuilder() Builder {
	return NewKetamaPickBuilder(KetamaOptions{Weighted: true, Hash: KetamaHashMD5})
}
