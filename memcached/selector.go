package memcached

import "fmt"

// ServerSelector picks a configured server for a key. Implementations must be
// safe for concurrent use and treat each PickServers snapshot as immutable.
// When set on Config.Cluster.Selector, it replaces built-in Rendezvous+XXH3 routing.
type ServerSelector interface {
	// PickServers selects the server for key from the current membership snapshot.
	// servers is never empty; return an element of servers or an error.
	PickServers(servers []Server, key []byte) (Server, error)
}

var (
	_ builder = selectorBuilder{}
	_ picker  = (*selectorPicker)(nil)
)

// selectorBuilder adapts a public ServerSelector into the internal builder.
type selectorBuilder struct {
	sel ServerSelector
}

func (b selectorBuilder) Build(_ []*addr) picker {
	return &selectorPicker{sel: b.sel}
}

type selectorPicker struct {
	sel ServerSelector
}

func (p *selectorPicker) Pick(addrs []*addr, key []byte) (*addr, error) {
	if p == nil || p.sel == nil {
		return nil, fmt.Errorf("nil selector: %w", ErrInvalidArgument)
	}
	servers := addrsToServers(addrs)
	got, err := p.sel.PickServers(servers, key)
	if err != nil {
		return nil, err
	}
	wantNet := serverNetwork(got)
	for _, a := range addrs {
		if a != nil && a.Address == got.Address && serverNetwork(addrToServer(a)) == wantNet {
			return a, nil
		}
	}
	return nil, fmt.Errorf("selector returned unknown server %s: %w", got.Address, ErrInvalidAddress)
}
