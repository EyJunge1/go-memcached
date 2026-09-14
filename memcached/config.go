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

// TimeoutNone leaves a Transport timeout unlimited aside from the caller's context.
// A zero DialTimeout, ReadTimeout, or WriteTimeout still selects the package default.
const TimeoutNone time.Duration = -1

// Config is the sole input to New.
// Zero or nil turns optional features off (Failover, Auth). A pointer field
// is used when 0 is a legal wire token (GetOpts.Touch, FlushOpts.Delay);
// otherwise a zero value omits the flag (MetaGet.Recache). Numeric zeros
// become package defaults (DialTimeout 3s); TimeoutNone skips the extra
// deadline. Features that default on use an inverted bool (DisableTCPNoDelay).
type Config struct {
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
}

// Server names one cluster member.
type Server struct {
	// Network is empty/"tcp" by default, or unix, udp, and the dual-stack
	// forms tcp4, tcp6, udp4, udp6.
	Network string
	Address string
	Weight  uint32 // 0 is treated as 1 by Ketama
}

// Transport configures dialing, TLS, and I/O deadlines.
// Zero timeouts become DialTimeout 3s and Read/WriteTimeout 5s; RetryAttempts 0 becomes 1.
// TimeoutNone skips the extra deadline. Dial is the raw socket; a non-nil TLS wraps it.
type Transport struct {
	TLS               *tls.Config
	Dial              DialContextFunc
	DialTimeout       time.Duration // 0 becomes 3s; TimeoutNone skips the extra deadline
	ReadTimeout       time.Duration // 0 becomes 5s; TimeoutNone skips the extra deadline
	WriteTimeout      time.Duration // 0 becomes 5s; TimeoutNone skips the extra deadline
	KeepAlive         time.Duration // 0 leaves net.Dialer's keep-alive; not TimeoutNone
	RetryAttempts     int           // 0 becomes 1
	AllowInsecureTLS  bool
	DisableTCPNoDelay bool // false keeps TCP_NODELAY enabled
	// RequireTLS rejects New unless TLS is present. The transport default stays plaintext.
	RequireTLS bool
	// UnixSocketAnyMode accepts sockets that are other-writable (mode bit 002).
	UnixSocketAnyMode bool
	// UnixSocketMaxMode, if set, rejects sockets whose mode has bits outside the mask.
	UnixSocketMaxMode os.FileMode
}

// Pool sizes the per-node connection set.
// Zero MaxOpen/MaxIdle become 100/10. Negative MaxIdle keeps nothing idle.
// Zero MaxLifetime, MaxIdleTime, and MinIdle mean no age limit and no warmup.
type Pool struct {
	MinIdle     int
	MaxIdle     int
	MaxOpen     int
	MaxLifetime time.Duration
	MaxIdleTime time.Duration
}

// Cluster holds the key namespace and optional DNS refresh.
// The picker is always weighted Ketama/MD5.
type Cluster struct {
	// Namespace is prepended on the wire only (250-byte key limit includes it).
	// Dumps and watch streams still show the wired key.
	Namespace string
	// ResolverRefresh > 0 re-resolves ResolveSpec (or formatted Servers) on that interval.
	// AllowSetServers is required.
	ResolverRefresh time.Duration
	ResolveSpec     string // refreshed on each tick; empty uses formatted Servers

	pickBuilder builder
	resolver    resolver
}

// Failover skips a node after consecutive transport errors.
// After FailureLimit the node is ignored for RetryAfter, then DeadAfter.
// Zero RetryAfter and DeadAfter keep the node down until a later request succeeds.
type Failover struct {
	FailureLimit int
	RetryAfter   time.Duration
	DeadAfter    time.Duration
	// Eject removes a node from the picker after FailureLimit. Without Eject,
	// Get still hits a quarantined node; other ops skip it.
	Eject bool
}

// Auth is text-protocol token authentication (-Y).
type Auth struct {
	User, Pass     string
	AllowPlaintext bool
}

// ParseServers splits a comma-separated spec (host:port, unix://, udp://, ?weight=).
// Hosts are resolved with DNS. A hand-built []Server is used as written.
func ParseServers(spec string) ([]Server, error) {
	addrs, err := defaultResolver{}.Resolve(spec)
	if err != nil {
		return nil, err
	}
	return addrsToServers(addrs), nil
}

// addrsToServers converts resolved cluster addrs into the public Server list.
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

// addrToServer copies one cluster addr into a Server.
func addrToServer(a *addr) Server {
	return Server{Network: a.Network, Address: a.Address, Weight: a.Weight}
}

// serversToAddrs validates Config servers and builds the corresponding cluster addrs.
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

// formatServerSpec renders servers as a comma-separated ParseServers spec.
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

// serverNetwork returns s.Network, or "tcp" when the field is empty.
func serverNetwork(s Server) string {
	if s.Network == "" {
		return "tcp"
	}
	return s.Network
}

// isLoopbackServer reports whether s is a Unix socket, localhost, or a loopback IP.
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

// isStreamNetwork reports whether network is TCP or Unix (including the empty TCP default).
func isStreamNetwork(network string) bool {
	switch network {
	case "", "tcp", "tcp4", "tcp6", "unix":
		return true
	default:
		return false
	}
}

// serversAreUDP reports whether servers use UDP. Call after validateTransportServers.
func serversAreUDP(servers []Server) bool {
	for _, s := range servers {
		if isUDPNetwork(serverNetwork(s)) {
			return true
		}
	}
	return false
}

// parseServerSpec turns a ResolveSpec into Servers without DNS (unix://, udp://, else tcp).
func parseServerSpec(spec string) ([]Server, error) {
	if spec == "" {
		return nil, fmt.Errorf("empty address: %w", ErrInvalidAddress)
	}
	out := make([]Server, 0, 4)
	for part := range strings.SplitSeq(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		raw, weight, err := splitAddrWeight(part)
		if err != nil {
			return nil, err
		}
		network, addr := "tcp", raw
		if strings.HasPrefix(raw, "unix://") {
			network = "unix"
			addr = strings.TrimPrefix(raw, "unix://")
		} else if strings.HasPrefix(raw, "udp://") {
			network = "udp"
			addr = strings.TrimPrefix(raw, "udp://")
		}
		if strings.TrimSpace(addr) == "" {
			return nil, fmt.Errorf("empty address: %w", ErrInvalidAddress)
		}
		out = append(out, Server{Network: network, Address: addr, Weight: weight})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no available address: %w", ErrInvalidAddress)
	}
	return out, nil
}

// validateTransportServers rejects mixed UDP/stream clusters and TLS on UDP.
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

// validateAuthAgainstServers rejects token auth on UDP and plaintext auth to non-loopback hosts.
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

// validateConfig checks Config invariants before New builds a Client.
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
	if cfg.Cluster.ResolveSpec != "" {
		specServers, err := parseServerSpec(cfg.Cluster.ResolveSpec)
		if err != nil {
			return err
		}
		if err := validateTransportServers(specServers, cfg.Transport.TLS); err != nil {
			return err
		}
		if serversAreUDP(cfg.Servers) != serversAreUDP(specServers) {
			return fmt.Errorf("ResolveSpec transport differs from Servers: %w", ErrMixedTransport)
		}
		if cfg.Auth != nil {
			if err := validateAuthAgainstServers(specServers, cfg.Auth.AllowPlaintext, cfg.Transport.TLS != nil); err != nil {
				return err
			}
		}
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

// DefaultFailover returns the former multi-node policy: two failures, 1s retry, 30s dead, eject on.
func DefaultFailover() Failover {
	return Failover{
		FailureLimit: 2,
		RetryAfter:   time.Second,
		DeadAfter:    30 * time.Second,
		Eject:        true,
	}
}
