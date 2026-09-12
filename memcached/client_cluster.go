package memcached

import (
	"fmt"
	"net"
	"strings"
)

var _ resolver = defaultResolver{}

type (
	// resolver turns a server spec into addresses.
	resolver interface {
		Resolve(spec string) ([]*addr, error)
	}

	// picker chooses a node for a key.
	picker interface {
		Pick(addrs []*addr, key []byte) (*addr, error)
	}

	indexedPicker interface {
		PickIndex(addrs []*addr, key []byte) (int, *addr, error)
	}
)

type (
	// builder constructs a picker for a snapshot of addrs.
	builder interface {
		Build(addrs []*addr) picker
	}

	// defaultResolver parses host:port, unix://, and udp:// specs.
	defaultResolver struct{}
)

// Resolve parses a comma-separated server spec into addrs.
func (r defaultResolver) Resolve(spec string) ([]*addr, error) {
	if spec == "" {
		return nil, fmt.Errorf("empty address: %w", ErrInvalidAddress)
	}

	addrs := strings.Split(spec, ",")
	result := make([]*addr, 0, len(addrs))

	for _, address := range addrs {
		address = strings.TrimSpace(address)
		if address == "" {
			continue
		}

		raw, err := cleanAddr(address)
		if err != nil {
			return nil, err
		}

		network, resolvedAddr, err := r.resolveAddr(raw)
		if err != nil {
			return nil, err
		}

		result = append(result, newAddr(network, resolvedAddr))
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("no available address: %w", ErrInvalidAddress)
	}

	return result, nil
}

// resolveAddr maps one host, unix://, or udp:// token onto a network and address.
func (r defaultResolver) resolveAddr(address string) (network, addr string, err error) {
	address = strings.TrimSpace(address)
	if address == "" {
		return "", "", fmt.Errorf("empty address: %w", ErrInvalidAddress)
	}

	network, addr = "tcp", address
	switch {
	case strings.HasPrefix(address, "unix://"):
		network = "unix"
		addr = strings.TrimPrefix(address, "unix://")
	case strings.HasPrefix(address, "udp://"):
		network = "udp"
		addr = strings.TrimPrefix(address, "udp://")
	}

	switch network {
	case "tcp":
		_, err = net.ResolveTCPAddr(network, addr)
	case "udp":
		_, err = net.ResolveUDPAddr(network, addr)
	case "unix":
		_, err = net.ResolveUnixAddr(network, addr)
	}
	if err != nil {
		return "", "", fmt.Errorf("invalid address: %s: %w", address, err)
	}
	return network, addr, nil
}

func pickAddrIndex(p picker, addrs []*addr, key []byte) (int, *addr, error) {
	if indexed, ok := p.(indexedPicker); ok {
		i, node, err := indexed.PickIndex(addrs, key)
		if err != nil {
			return 0, nil, err
		}
		if i < 0 || i >= len(addrs) || addrs[i] != node {
			return 0, nil, fmt.Errorf(
				"indexed picker returned an address outside its snapshot: %w",
				ErrInvalidAddress,
			)
		}
		return i, node, nil
	}
	node, err := p.Pick(addrs, key)
	if err != nil {
		return 0, nil, err
	}
	i, err := pipelineAddrIndex(addrs, node)
	if err != nil {
		return 0, nil, err
	}
	return i, node, nil
}

// cleanAddr rejects address query parameters (legacy ?weight= is unsupported).
func cleanAddr(address string) (string, error) {
	if strings.Contains(address, "?") {
		return "", fmt.Errorf("address query parameters are not supported: %w", ErrInvalidAddress)
	}
	return address, nil
}
