package memcached

import (
	"context"
	"net"
	"time"
)

type addr struct {
	Network string
	Address string
	Weight  uint32 // 0 ⇒ 1; used by Ketama
}

func newAddrWeighted(network, address string, weight uint32) *addr {
	return &addr{
		Network: network,
		Address: address,
		Weight:  weight,
	}
}

func (a *addr) ketamaWeight() uint32 {
	if a == nil || a.Weight == 0 {
		return 1
	}
	return a.Weight
}

func (a *addr) id() string {
	return a.Network + "\x00" + a.Address
}

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
