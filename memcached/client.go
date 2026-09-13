package memcached

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Client is a concurrent memcached client with a pool per server.
// With more than one node, each key is hashed onto a member.
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

// New builds a Client from Config.
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

// SetServers swaps cluster membership. Config.AllowSetServers must be set.
func (c *Client) SetServers(servers []Server) error {
	if c.rt == nil || !c.rt.allowSetServers {
		return ErrSetServersNotAllowed
	}
	return c.applyServers(servers, true)
}

// applyServers swaps cluster membership and optionally updates the resolve spec.
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

// Close shuts idle sockets and stops resolver refresh. TCP and Unix send quit first; UDP does not.
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

// snapshotCluster copies the current address list and picker under read lock.
func (c *Client) snapshotCluster() ([]*addr, picker) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.addrs, c.picker
}

// Servers returns a snapshot of the configured members.
func (c *Client) Servers() []Server {
	addrs, _ := c.snapshotCluster()
	return addrsToServers(addrs)
}

// ResolverErr returns the most recent periodic resolver failure, if any.
func (c *Client) ResolverErr() error {
	c.resolverMu.Lock()
	defer c.resolverMu.Unlock()
	return c.resolverErr
}

// serverKey identifies a Server by Network and Address for membership maps.
func serverKey(s Server) string {
	return serverNetwork(s) + "\x00" + s.Address
}

// serverIndex maps each configured member to its position in Servers().
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

// pickFirstLiveAddr returns the first live member, or the first if all are dead. An empty cluster is ErrInvalidAddress.
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

// setResolverErr stores the latest periodic resolver failure.
func (c *Client) setResolverErr(err error) {
	c.resolverMu.Lock()
	c.resolverErr = err
	c.resolverMu.Unlock()
}

// PoolStat is one node's connection-pool occupancy.
type PoolStat struct {
	Server  Server
	Total   int
	Idle    int
	MaxOpen int
	MaxIdle int
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
			Server:  addrToServer(addr),
			Total:   s.TotalConns,
			Idle:    s.IdleConns,
			MaxOpen: s.MaxOpen,
			MaxIdle: s.MaxIdle,
		})
	}
	return out
}

// nsBytes prefixes key with Config.Cluster.Namespace.
func (c *Client) nsBytes(key []byte) []byte {
	if c.rt == nil || c.rt.namespace == "" {
		return key
	}
	out := make([]byte, 0, len(c.rt.namespace)+len(key))
	out = append(out, c.rt.namespace...)
	return append(out, key...)
}

// stripNS removes the configured namespace from a returned key.
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
