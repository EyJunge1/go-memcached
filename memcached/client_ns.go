package memcached

import (
	"bytes"
	"sync"
)

// PoolStat is one node's connection-pool occupancy and reuse counters.
type PoolStat struct {
	Server     Server
	Total      int
	Idle       int
	MaxOpen    int
	MaxIdle    int
	IdleHits   int64 // LIFO idle reuse
	IdleMisses int64 // dial path
	Evicted    int64 // closed at idle cap + age + lifetime
}

// PoolStats reports pool occupancy for each configured node. Members that
// have never been dialed are skipped.
func (c *Client) PoolStats() []PoolStat {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]PoolStat, 0, len(c.connPools))
	for _, addr := range c.addrs {
		if addr == nil {
			continue
		}
		pool, ok := c.connPools[addr.id()]
		if !ok {
			continue
		}
		s := pool.stats()
		out = append(out, PoolStat{
			Server:     addrToServer(addr),
			Total:      s.TotalConns,
			Idle:       s.IdleConns,
			MaxOpen:    s.MaxOpen,
			MaxIdle:    s.MaxIdle,
			IdleHits:   s.IdleHits,
			IdleMisses: s.IdleMisses,
			Evicted:    s.closedIdleCap + s.closedIdleAge + s.closedLifetime,
		})
	}
	return out
}

// nsKey prefixes key with Config.Cluster.Namespace.
// The box must be adopted by a request or returned with putBuilderBox.
func (c *Client) nsKey(key []byte) (wired []byte, box *[]byte) {
	if c == nil || c.rt == nil || len(c.rt.nsPrefix) == 0 {
		return key, nil
	}
	need := len(c.rt.nsPrefix) + len(key)
	box = takeBuilderBox()
	buf := (*box)[:0]
	if cap(buf) < need {
		buf = make([]byte, 0, need)
	}
	buf = append(buf, c.rt.nsPrefix...)
	buf = append(buf, key...)
	*box = buf
	return buf, box
}

// stripNS removes the configured namespace from a returned key.
func (c *Client) stripNS(key []byte) []byte {
	if key == nil {
		return nil
	}
	if c == nil || c.rt == nil || len(c.rt.nsPrefix) == 0 {
		return key
	}
	if !bytes.HasPrefix(key, c.rt.nsPrefix) {
		return key
	}
	return key[len(c.rt.nsPrefix):]
}

// wireGetKeys prefixes keys with the namespace when one is configured.
// The caller must defer releaseWiredGetKeys(box, hdrp). On error both are nil.
func (c *Client) wireGetKeys(keys [][]byte) (wired [][]byte, box *[]byte, hdrp *[][]byte, err error) {
	if !c.hasNS() {
		for _, key := range keys {
			if err := validateKeyAndValue(key, nil); err != nil {
				return nil, nil, nil, err
			}
		}
		return keys, nil, nil, nil
	}
	n := len(keys)
	need := len(c.rt.nsPrefix) * n
	for _, k := range keys {
		need += len(k)
	}
	box = takeBuilderBox()
	buf := (*box)[:0]
	if cap(buf) < need {
		buf = make([]byte, 0, need)
	}
	var ok bool
	hdrp, ok = _nsHdrPool.Get().(*[][]byte)
	if !ok || hdrp == nil {
		s := make([][]byte, 0, 8)
		hdrp = &s
	}
	if cap(*hdrp) < n {
		*hdrp = make([][]byte, n)
	}
	*hdrp = (*hdrp)[:n]
	hdr := *hdrp
	for i, key := range keys {
		start := len(buf)
		buf = append(buf, c.rt.nsPrefix...)
		buf = append(buf, key...)
		wiredKey := buf[start:]
		if err := validateKeyAndValue(wiredKey, nil); err != nil {
			putBuilderBox(box)
			*hdrp = (*hdrp)[:0]
			_nsHdrPool.Put(hdrp)
			return nil, nil, nil, err
		}
		hdr[i] = wiredKey
	}
	*box = buf
	*hdrp = hdr
	return hdr, box, hdrp, nil
}

var _nsHdrPool = sync.Pool{
	New: func() any {
		s := make([][]byte, 0, 8)
		return &s
	},
}

func releaseWiredGetKeys(box *[]byte, hdrp *[][]byte) {
	putBuilderBox(box)
	if hdrp != nil {
		*hdrp = (*hdrp)[:0]
		_nsHdrPool.Put(hdrp)
	}
}
