package memcached

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

var (
	_ resolver = defaultResolver{}
	_ builder  = ketamaPickBuilder{}
	_ picker   = &ketamaPicker{}
)

// resolver turns a server spec into addresses.
type resolver interface {
	Resolve(spec string) ([]*addr, error)
}

// picker chooses a node for a key.
type picker interface {
	Pick(addrs []*addr, key []byte) (*addr, error)
}

type indexedPicker interface {
	PickIndex(addrs []*addr, key []byte) (int, *addr, error)
}

func pickAddrIndex(p picker, addrs []*addr, key []byte) (int, *addr, error) {
	if indexed, ok := p.(indexedPicker); ok {
		i, node, err := indexed.PickIndex(addrs, key)
		if err != nil {
			return 0, nil, err
		}
		if i < 0 || i >= len(addrs) || addrs[i] != node {
			panic("memcached: indexed picker returned an address outside its snapshot")
		}
		return i, node, nil
	}
	node, err := p.Pick(addrs, key)
	if err != nil {
		return 0, nil, err
	}
	return pipelineAddrIndex(addrs, node), node, nil
}

// builder constructs a picker for a snapshot of addrs.
type builder interface {
	Build(addrs []*addr) picker
}

// defaultResolver parses host:port, unix://, and udp:// specs.
type defaultResolver struct{}

// Resolve parses a comma-separated server spec into weighted addrs.
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

		raw, weight, err := splitAddrWeight(address)
		if err != nil {
			return nil, err
		}

		network, resolvedAddr, err := r.resolveAddr(raw)
		if err != nil {
			return nil, err
		}

		result = append(result, newAddrWeighted(network, resolvedAddr, weight))
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("no available address: %w", ErrInvalidAddress)
	}

	return result, nil
}

// splitAddrWeight pulls an optional ?weight=N suffix. Unknown query keys fail.
func splitAddrWeight(address string) (raw string, weight uint32, err error) {
	before, after, ok := strings.Cut(address, "?")
	if !ok {
		return address, 0, nil
	}
	raw, query := before, after
	// unix:///path/?weight= leaves a trailing slash on the path.
	raw = strings.TrimSuffix(raw, "/")
	if raw == "" {
		return "", 0, fmt.Errorf("empty address: %w", ErrInvalidAddress)
	}
	values, err := url.ParseQuery(query)
	if err != nil {
		return "", 0, fmt.Errorf("invalid address: %s: %w", address, ErrInvalidAddress)
	}
	for k := range values {
		if k != "weight" {
			return "", 0, fmt.Errorf("invalid address query %q: %w", k, ErrInvalidAddress)
		}
	}
	if !values.Has("weight") {
		return raw, 0, nil
	}
	n, err := strconv.ParseUint(values.Get("weight"), 10, 32)
	if err != nil {
		return "", 0, fmt.Errorf("invalid address weight: %s: %w", address, ErrInvalidAddress)
	}
	return raw, uint32(n), nil
}

// resolveAddr maps one host, unix://, or udp:// token onto a network and address.
func (r defaultResolver) resolveAddr(address string) (network, addr string, err error) {
	address = strings.TrimSpace(address)
	if address == "" {
		return "", "", fmt.Errorf("empty address: %w", ErrInvalidAddress)
	}

	network = "tcp"
	addr = address

	if strings.HasPrefix(address, "unix://") {
		network = "unix"
		addr = strings.TrimPrefix(address, "unix://")
	} else if strings.HasPrefix(address, "udp://") {
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
