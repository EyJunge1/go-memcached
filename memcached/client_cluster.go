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

type resolver interface {
	Resolve(spec string) ([]*addr, error)
}

type picker interface {
	Pick(addrs []*addr, key []byte) (*addr, error)
}

type builder interface {
	Build(addrs []*addr) picker
}

type defaultResolver struct{}

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

// splitAddrWeight strips an optional ?weight=N query. Unknown query keys are rejected.
func splitAddrWeight(address string) (raw string, weight uint32, err error) {
	q := strings.Index(address, "?")
	if q < 0 {
		return address, 0, nil
	}
	raw, query := address[:q], address[q+1:]
	// Accept an extra slash before ?weight=.
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
