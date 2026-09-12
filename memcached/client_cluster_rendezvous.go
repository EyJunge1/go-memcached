package memcached

import (
	"fmt"

	"github.com/zeebo/xxh3"
)

var (
	_ builder       = rendezvousPickBuilder{}
	_ picker        = (*rendezvousPicker)(nil)
	_ indexedPicker = (*rendezvousPicker)(nil)
)

// defaultPickBuilder returns the Rendezvous+XXH3 picker used by New.
func defaultPickBuilder() builder {
	return rendezvousPickBuilder{}
}

// rendezvousPickBuilder builds an XXH3 rendezvous picker.
type rendezvousPickBuilder struct{}

// Build returns a rendezvousPicker for addrs.
func (rendezvousPickBuilder) Build(_ []*addr) picker {
	return &rendezvousPicker{}
}

// rendezvousPicker selects the node with the highest XXH3 score.
// Score is HashSeed(key, hash(nodeID)) — alloc-free HRW with a precomputed node seed.
type rendezvousPicker struct{}

// Pick selects the cluster member that owns key under rendezvous hashing.
func (p *rendezvousPicker) Pick(addrs []*addr, key []byte) (*addr, error) {
	_, node, err := p.PickIndex(addrs, key)
	return node, err
}

// PickIndex selects the owner and returns its index in addrs.
func (p *rendezvousPicker) PickIndex(addrs []*addr, key []byte) (int, *addr, error) {
	n := len(addrs)
	if n == 0 {
		return 0, nil, fmt.Errorf("no available address: %w", ErrInvalidAddress)
	}
	if n == 1 {
		return 0, addrs[0], nil
	}

	bestIdx := -1
	var bestScore uint64
	var bestID string
	for i, a := range addrs {
		if a == nil {
			continue
		}
		score := xxh3.HashSeed(key, a.hashSeed)
		if bestIdx < 0 || score > bestScore {
			bestScore = score
			bestIdx = i
			bestID = a.ident
			continue
		}
		if score == bestScore {
			if id := a.ident; id > bestID {
				bestIdx = i
				bestID = id
			}
		}
	}
	if bestIdx < 0 {
		return 0, nil, fmt.Errorf("no available address: %w", ErrInvalidAddress)
	}
	return bestIdx, addrs[bestIdx], nil
}
