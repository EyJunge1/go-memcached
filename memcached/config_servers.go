package memcached

import (
	"crypto/tls"
	"fmt"
	"net"
	"strings"
)

// ParseServers splits a comma-separated spec (host:port, unix://, udp://).
// Hosts are resolved with DNS. A hand-built []Server is used as written.
func ParseServers(spec string) ([]Server, error) {
	var resolver defaultResolver
	addrs, err := resolver.Resolve(spec)
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
	return Server{Network: a.Network, Address: a.Address}
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
		a := newAddr(network, s.Address)
		a.slot = len(out)
		out = append(out, a)
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
		raw, err := cleanAddr(part)
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
		out = append(out, Server{Network: network, Address: addr})
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
