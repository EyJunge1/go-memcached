package memcached

import (
	"context"
	"net"
	"strconv"
	"time"
)

// Addr represents a memcached server address.
type Addr struct {
	Network string // Network representation of the address
	Address string // Address representation of the address

	// Priority of the address in the cluster. It is used to pick the address
	// in rendezvous hashing picker. The higher the priority means the node would
	// win the competition while hash score is the same.
	//
	// By default, the priority is set by the order of the address in the cluster; the first address
	// has the lowest priority, the last address has the highest.
	//
	// If you want to customize resolver, be careful to set the priority, make sure the priority
	// is unique.
	Priority int

	// Weight is used by Ketama consistent hashing. Zero means 1.
	// Priority is not a weight.
	Weight uint32

	metadata map[string]any
}

// NewAddr creates a new Addr with the given network, address and priority.
func NewAddr(network, address string, priority int) *Addr {
	return NewAddrWeighted(network, address, priority, 0)
}

// NewAddrWeighted creates a new Addr with an optional Ketama weight (0 means 1).
func NewAddrWeighted(network, address string, priority int, weight uint32) *Addr {
	return &Addr{
		Network:  network,
		Address:  address,
		Priority: priority,
		Weight:   weight,
		metadata: make(map[string]any, 2),
	}
}

func (a *Addr) ketamaWeight() uint32 {
	if a == nil || a.Weight == 0 {
		return 1
	}
	return a.Weight
}

func (a *Addr) shortcut() []byte {
	return []byte(a.Network + "-" + a.Address + strconv.Itoa(a.Priority))
}

// GetMetadata returns the metadata value by the given key.
func (a *Addr) GetMetadata(mdKey string) any {
	return a.metadata[mdKey]
}

// Add adds the metadata key-value pair to the Addr.
func (a *Addr) Add(mdKey string, mdValue any) {
	a.metadata[mdKey] = mdValue
}

func (a *Addr) id() string {
	return a.Network + "\x00" + a.Address
}

func (a *Addr) dial(ctx context.Context, dialTimeout, keepAlive time.Duration, dialFn DialContextFunc) (net.Conn, error) {
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
