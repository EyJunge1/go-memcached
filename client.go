package memcached

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Client is a thread-safe memcached client. It manages a connection pool per
// server address and, in cluster mode, picks a node per key.
type Client struct {
	options *clientOptions

	mu         sync.RWMutex
	addrs      []*Addr
	picker     Picker
	connPools  map[string]*connPool
	addrString string

	healthMu  sync.Mutex
	failCount map[string]int
	deadUntil map[string]time.Time

	refreshStop chan struct{}
	refreshOnce sync.Once
}

// New creates a memcached client for the given address and options.
//
// The address may be a single node or a comma-separated cluster list.
// The same key is always sent to the same instance. Gets, GetAndTouches,
// and GetMulti group keys by node and fetch them in parallel.
func New(addr string, opts ...ClientOption) (*Client, error) {
	options := newClientOptions()
	for _, opt := range opts {
		opt(options)
	}
	if err := applyTLSDialer(options); err != nil {
		return nil, err
	}

	addrs, err := options.resolver.Resolve(addr)
	if err != nil {
		return nil, fmt.Errorf("resolve failed: %w", err)
	}

	if len(addrs) == 0 {
		return nil, fmt.Errorf("empty address: %w", ErrInvalidAddress)
	}

	c := &Client{
		options:    options,
		addrs:      addrs,
		picker:     options.pickBuilder.Build(addrs),
		connPools:  make(map[string]*connPool, 4),
		addrString: addr,
		failCount:  make(map[string]int),
		deadUntil:  make(map[string]time.Time),
	}
	c.startResolverRefresh()
	return c, nil
}

// SetServers replaces the cluster address list. Existing pools for hosts
// that remain are reused; pools for removed hosts are closed.
func (c *Client) SetServers(addr string) error {
	addrs, err := c.options.resolver.Resolve(addr)
	if err != nil {
		return fmt.Errorf("resolve failed: %w", err)
	}
	if len(addrs) == 0 {
		return fmt.Errorf("empty address: %w", ErrInvalidAddress)
	}

	picker := c.options.pickBuilder.Build(addrs)
	keep := make(map[string]struct{}, len(addrs))
	for _, a := range addrs {
		keep[a.id()] = struct{}{}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	next := make(map[string]*connPool, len(addrs))
	for key, pool := range c.connPools {
		if _, ok := keep[key]; ok {
			next[key] = pool
			continue
		}
		_ = pool.close()
	}

	c.addrs = addrs
	c.picker = picker
	c.connPools = next
	c.addrString = addr
	return nil
}

func (c *Client) Close() error {
	c.refreshOnce.Do(func() {
		if c.refreshStop != nil {
			close(c.refreshStop)
		}
	})

	c.mu.Lock()
	defer c.mu.Unlock()

	var errs []error
	for _, pool := range c.connPools {
		if err := pool.close(); err != nil {
			errs = append(errs, err)
		}
	}
	c.connPools = make(map[string]*connPool)
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("Close: %w", err)
	}
	return nil
}

func (c *Client) snapshotCluster() ([]*Addr, Picker) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.addrs, c.picker
}

// Servers returns a copy of the configured cluster addresses.
func (c *Client) Servers() []Addr {
	addrs, _ := c.snapshotCluster()
	out := make([]Addr, 0, len(addrs))
	for _, a := range addrs {
		if a == nil {
			continue
		}
		cp := *a
		cp.metadata = nil
		out = append(out, cp)
	}
	return out
}

// PoolStat is a snapshot of one node's connection pool.
type PoolStat struct {
	Address  string
	Total    int
	Idle     int
	MaxConns int
	MaxIdle  int
}

// PoolStats returns pool occupancy per configured node. Nodes that have never
// been dialed are omitted.
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
			Address:  addr.Address,
			Total:    s.TotalConns,
			Idle:     s.IdleConns,
			MaxConns: s.MaxConns,
			MaxIdle:  s.MaxIdle,
		})
	}
	return out
}

func (c *Client) nsString(key string) string {
	if c.options == nil || c.options.namespace == "" {
		return key
	}
	return c.options.namespace + key
}

func (c *Client) nsBytes(key []byte) []byte {
	if c.options == nil || c.options.namespace == "" {
		return key
	}
	out := make([]byte, 0, len(c.options.namespace)+len(key))
	out = append(out, c.options.namespace...)
	return append(out, key...)
}

func (c *Client) hashKey(wire []byte) []byte {
	if c.options == nil || c.options.namespace == "" || c.options.hashWithPrefix {
		return wire
	}
	return bytes.TrimPrefix(wire, []byte(c.options.namespace))
}
