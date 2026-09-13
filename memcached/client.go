package memcached

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Client is a thread-safe memcached client. It manages a connection pool per
// server address and, in cluster mode, picks a node per key.
type Client struct {
	options *clientOptions

	mu               sync.RWMutex
	addrs            []*Addr
	picker           Picker
	connPools        map[string]*connPool
	resolveSpec      string
	resolveSpecFixed bool

	healthMu  sync.Mutex
	failCount map[string]int
	deadUntil map[string]time.Time

	refreshStop chan struct{}
	refreshOnce sync.Once

	resolverMu  sync.Mutex
	resolverErr error
}

// New creates a memcached client from Config.
func New(cfg Config) (*Client, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	addrs, err := serversToAddrs(cfg.Servers)
	if err != nil {
		return nil, err
	}
	options, err := optionsFromConfig(cfg)
	if err != nil {
		return nil, err
	}

	resolveSpec := cfg.Cluster.ResolveSpec
	resolveSpecFixed := resolveSpec != ""
	if resolveSpec == "" {
		resolveSpec = formatServerSpec(cfg.Servers)
	}

	c := &Client{
		options:          options,
		addrs:            addrs,
		picker:           options.pickBuilder.Build(addrs),
		connPools:        make(map[string]*connPool, 4),
		resolveSpec:      resolveSpec,
		resolveSpecFixed: resolveSpecFixed,
		failCount:        make(map[string]int),
		deadUntil:        make(map[string]time.Time),
	}
	c.startResolverRefresh()
	return c, nil
}

// SetServers replaces the cluster address list. Requires Config.AllowSetServers.
// Existing pools for hosts that remain are reused; pools for removed hosts are
// closed. Health state for removed IDs is dropped.
func (c *Client) SetServers(servers []Server) error {
	if c.options == nil || !c.options.allowSetServers {
		return ErrSetServersNotAllowed
	}
	return c.applyServers(servers, true)
}

func (c *Client) applyServers(servers []Server, updateSpec bool) error {
	var tlsCfg *tls.Config
	if c.options != nil && c.options.tlsSet {
		tlsCfg = c.options.tlsConfig
	}
	if err := validateTransportServers(servers, tlsCfg); err != nil {
		return err
	}
	if c.options != nil && (c.options.authUser != "" || c.options.authPass != "") {
		if err := validateAuthAgainstServers(servers, c.options.authAllowPlain, c.options.tlsSet); err != nil {
			return err
		}
	}
	addrs, err := serversToAddrs(servers)
	if err != nil {
		return err
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

	c.healthMu.Lock()
	for id := range c.failCount {
		if _, ok := keep[id]; !ok {
			delete(c.failCount, id)
		}
	}
	for id := range c.deadUntil {
		if _, ok := keep[id]; !ok {
			delete(c.deadUntil, id)
		}
	}
	c.healthMu.Unlock()

	c.addrs = addrs
	c.picker = picker
	c.connPools = next
	if updateSpec && !c.resolveSpecFixed {
		c.resolveSpec = formatServerSpec(servers)
	}
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

// Servers returns a copy of the configured cluster.
func (c *Client) Servers() []Server {
	addrs, _ := c.snapshotCluster()
	return addrsToServers(addrs)
}

// LastResolverError returns the last error from periodic resolver refresh.
func (c *Client) LastResolverError() error {
	c.resolverMu.Lock()
	defer c.resolverMu.Unlock()
	return c.resolverErr
}

func (c *Client) setResolverErr(err error) {
	c.resolverMu.Lock()
	c.resolverErr = err
	c.resolverMu.Unlock()
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
