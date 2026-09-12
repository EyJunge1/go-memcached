package memcached

import (
	"fmt"
	"strings"
)

// NewFromAddrs builds a Client from host specs (host:port, unix://, udp://).
// Specs are joined and parsed with ParseServers, then passed to New with default Config.
func NewFromAddrs(addrs ...string) (*Client, error) {
	if len(addrs) == 0 {
		return nil, fmt.Errorf("empty address: %w", ErrInvalidAddress)
	}
	for _, a := range addrs {
		if strings.TrimSpace(a) == "" {
			return nil, fmt.Errorf("empty address: %w", ErrInvalidAddress)
		}
	}
	servers, err := ParseServers(strings.Join(addrs, ","))
	if err != nil {
		return nil, err
	}
	return New(Config{Servers: servers})
}

// ConfigID returns the last committed Memorystore discovery ConfigID, or 0
// when discovery is off or nothing has committed yet.
func (c *Client) ConfigID() int64 {
	if c == nil || c.rt == nil {
		return 0
	}
	if ms, ok := c.rt.resolver.(*memorystoreResolver); ok {
		return ms.configID.Load()
	}
	return 0
}
