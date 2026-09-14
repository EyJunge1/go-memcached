package memcached

import (
	"context"
	"net"
	"sync/atomic"
	"time"
)

// addr is one cluster member after Config validation.
type addr struct {
	Network string
	Address string
	Weight  uint32 // 0 is treated as 1 by Ketama
	ident   string
	udp     bool
	pool    atomic.Pointer[connPool]
}

// newAddrWeighted returns a cluster addr with the given network, host, and Ketama weight.
func newAddrWeighted(network, address string, weight uint32) *addr {
	a := &addr{
		Network: network,
		Address: address,
		Weight:  weight,
		udp:     isUDPNetwork(network),
	}
	a.ident = network + "\x00" + address
	return a
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
	if a.ident == "" {
		a.ident = a.Network + "\x00" + a.Address
	}
	return a.ident
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
