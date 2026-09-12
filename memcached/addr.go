package memcached

import (
	"context"
	"net"
	"sync/atomic"
	"time"

	"github.com/zeebo/xxh3"
)

// addr is one cluster member after Config validation.
type addr struct {
	Network  string
	Address  string
	ident    string
	hashSeed uint64 // xxh3(ident); Rendezvous node seed for HashSeed(key, seed)
	udp      bool
	slot     int
	pool     atomic.Pointer[connPool]
}

// newAddr returns a cluster addr with the given network and host.
func newAddr(network, address string) *addr {
	a := &addr{
		Network: network,
		Address: address,
		udp:     isUDPNetwork(network),
	}
	a.ident = network + "\x00" + address
	a.hashSeed = xxh3.HashString(a.ident)
	return a
}

// id returns a stable node identity of network and address joined by a NUL.
func (a *addr) id() string {
	if a.ident == "" {
		a.ident = a.Network + "\x00" + a.Address
		a.hashSeed = xxh3.HashString(a.ident)
	}
	return a.ident
}

// dial opens a socket to this addr using dialFn when set, otherwise net.Dialer.
func (a *addr) dial(
	ctx context.Context,
	dialTimeout, keepAlive time.Duration,
	dialFn DialContextFunc,
) (net.Conn, error) {
	if dialFn != nil {
		if dialTimeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, dialTimeout)
			defer cancel()
		}
		return dialFn(ctx, a.Network, a.Address)
	}
	d := &net.Dialer{Timeout: dialTimeout, KeepAlive: keepAlive}
	return d.DialContext(ctx, a.Network, a.Address)
}
