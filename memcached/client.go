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
	rt *runtime

	mu               sync.RWMutex
	addrs            []*addr
	picker           picker
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

	closed bool
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
	rt, err := runtimeFromConfig(cfg)
	if err != nil {
		return nil, err
	}

	resolveSpec := cfg.Cluster.ResolveSpec
	resolveSpecFixed := resolveSpec != ""
	if resolveSpec == "" {
		resolveSpec = formatServerSpec(cfg.Servers)
	}

	c := &Client{
		rt:               rt,
		addrs:            addrs,
		picker:           rt.pickBuilder.Build(addrs),
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
	if c.rt == nil || !c.rt.allowSetServers {
		return ErrSetServersNotAllowed
	}
	return c.applyServers(servers, true)
}

func (c *Client) applyServers(servers []Server, updateSpec bool) error {
	var tlsCfg *tls.Config
	if c.rt != nil && c.rt.tlsSet {
		tlsCfg = c.rt.tlsConfig
	}
	if err := validateTransportServers(servers, tlsCfg); err != nil {
		return err
	}
	if c.rt != nil && (c.rt.authUser != "" || c.rt.authPass != "") {
		if err := validateAuthAgainstServers(servers, c.rt.authAllowPlain, c.rt.tlsSet); err != nil {
			return err
		}
	}
	var addrs []*addr
	if len(servers) > 0 {
		var err error
		addrs, err = serversToAddrs(servers)
		if err != nil {
			return err
		}
	}

	picker := c.rt.pickBuilder.Build(addrs)
	keep := make(map[string]struct{}, len(addrs))
	for _, a := range addrs {
		keep[a.id()] = struct{}{}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}

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

// Close closes idle connections and stops resolver refresh.
// On TCP/Unix it writes quit before closing; UDP skips quit (spec allows close-only).
// After Close, methods return an error. Close is safe to defer.
func (c *Client) Close() error {
	c.refreshOnce.Do(func() {
		if c.refreshStop != nil {
			close(c.refreshStop)
		}
	})

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true

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

func (c *Client) snapshotCluster() ([]*addr, picker) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.addrs, c.picker
}

// Servers returns a copy of the configured cluster.
func (c *Client) Servers() []Server {
	addrs, _ := c.snapshotCluster()
	return addrsToServers(addrs)
}

// ResolverErr returns the last error from periodic resolver refresh.
func (c *Client) ResolverErr() error {
	c.resolverMu.Lock()
	defer c.resolverMu.Unlock()
	return c.resolverErr
}

func serverKey(s Server) string {
	return serverNetwork(s) + "\x00" + s.Address
}

func (c *Client) serverIndex() map[string]int {
	addrs, _ := c.snapshotCluster()
	out := make(map[string]int, len(addrs))
	for i, a := range addrs {
		if a != nil {
			out[serverKey(addrToServer(a))] = i
		}
	}
	return out
}

// pickFirstLiveAddr returns the first Servers() entry that is not in
// DeadServers(). Identity is Network (empty = tcp) + Address. If every
// node is dead, it returns Servers()[0]. An empty cluster is ErrInvalidAddress.
func (c *Client) pickFirstLiveAddr() (*addr, error) {
	addrs, _ := c.snapshotCluster()
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no servers configured: %w", ErrInvalidAddress)
	}
	dead := make(map[string]struct{}, 4)
	for _, s := range c.DeadServers() {
		dead[serverKey(s)] = struct{}{}
	}
	var first *addr
	for _, a := range addrs {
		if a == nil {
			continue
		}
		if first == nil {
			first = a
		}
		if _, ok := dead[serverKey(addrToServer(a))]; !ok {
			return a, nil
		}
	}
	if first == nil {
		return nil, fmt.Errorf("no servers configured: %w", ErrInvalidAddress)
	}
	return first, nil
}

func (c *Client) setResolverErr(err error) {
	c.resolverMu.Lock()
	c.resolverErr = err
	c.resolverMu.Unlock()
}

// PoolStat is a snapshot of one node's connection pool.
type PoolStat struct {
	Server  Server
	Total   int
	Idle    int
	MaxOpen int
	MaxIdle int
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
			Server:  addrToServer(addr),
			Total:   s.TotalConns,
			Idle:    s.IdleConns,
			MaxOpen: s.MaxOpen,
			MaxIdle: s.MaxIdle,
		})
	}
	return out
}

func (c *Client) nsBytes(key []byte) []byte {
	if c.rt == nil || c.rt.namespace == "" {
		return key
	}
	out := make([]byte, 0, len(c.rt.namespace)+len(key))
	out = append(out, c.rt.namespace...)
	return append(out, key...)
}

func (c *Client) stripNS(key []byte) []byte {
	if c == nil || c.rt == nil || c.rt.namespace == "" {
		return key
	}
	prefix := []byte(c.rt.namespace)
	if !bytes.HasPrefix(key, prefix) {
		return key
	}
	out := make([]byte, len(key)-len(prefix))
	copy(out, key[len(prefix):])
	return out
}
