package memcached

import (
	"context"
	"net"
	"time"
)

// addr is one cluster member after Config validation.
type addr struct {
	Network string
	Address string
	Weight  uint32 // 0 is treated as 1 by Ketama
}

// newAddrWeighted returns a cluster addr with the given network, host, and Ketama weight.
func newAddrWeighted(network, address string, weight uint32) *addr {
	return &addr{
		Network: network,
		Address: address,
		Weight:  weight,
	}
}

// ketamaWeight returns the node's Ketama weight, treating a nil addr or zero weight as 1.
func (a *addr) ketamaWeight() uint32 {
	if a == nil || a.Weight == 0 {
		return 1
	}
	return a.Weight
}

// id returns a stable node identity of network and address joined by a NUL.
func (a *addr) id() string {
	return a.Network + "\x00" + a.Address
}

// dial opens a socket to this addr using dialFn when set, otherwise net.Dialer.
func (a *addr) dial(ctx context.Context, dialTimeout, keepAlive time.Duration, dialFn DialContextFunc) (net.Conn, error) {
	if dialFn != nil {
		if dialTimeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, dialTimeout)
			defer cancel()
		}
		return dialFn(ctx, a.Network, a.Address)
	}
	return (&net.Dialer{Timeout: dialTimeout, KeepAlive: keepAlive}).DialContext(ctx, a.Network, a.Address)
}
