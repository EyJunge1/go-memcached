package memcached

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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

	healthMu    sync.Mutex
	failCount   map[string]int
	deadUntil   map[string]time.Time
	healthDirty atomic.Bool
	deadDirty   atomic.Bool

	refreshStop chan struct{}
	refreshOnce sync.Once
	closeMu     sync.Mutex

	resolverMu  sync.Mutex
	resolverErr error

	closed         atomic.Bool
	single         atomic.Pointer[addr]
	clusterVersion atomic.Uint64
	healthVersion  atomic.Uint64
	meta           MetaClient
	fanout         *fanoutPool
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
	c.meta.c = c
	c.fanout = newFanoutPool(rt.fanoutWorkers)
	c.clusterVersion.Store(1)
	c.publishSingleLocked()
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
	if len(servers) > 0 && c.rt != nil && serversAreUDP(servers) != c.rt.udp {
		return fmt.Errorf("cannot change transport family: %w", ErrMixedTransport)
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
	if c.closed.Load() {
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
	healthChanged := false
	for id := range c.failCount {
		if _, ok := keep[id]; !ok {
			delete(c.failCount, id)
			healthChanged = true
		}
	}
	for id := range c.deadUntil {
		if _, ok := keep[id]; !ok {
			delete(c.deadUntil, id)
			healthChanged = true
		}
	}
	if healthChanged {
		c.healthVersion.Add(1)
	}
	c.healthDirty.Store(len(c.failCount) != 0)
	c.deadDirty.Store(len(c.deadUntil) != 0)
	c.healthMu.Unlock()

	c.addrs = addrs
	c.picker = picker
	c.connPools = next
	c.clusterVersion.Add(1)
	c.publishSingleLocked()
	for _, a := range addrs {
		if a == nil {
			continue
		}
		if p, ok := next[a.id()]; ok {
			a.pool.Store(p)
		}
	}
	if updateSpec && !c.resolveSpecFixed {
		c.resolveSpec = formatServerSpec(servers)
	}
	return nil
}

// Close shuts idle sockets and stops resolver refresh. TCP and Unix send quit first; UDP does not.
func (c *Client) Close() error {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	c.refreshOnce.Do(func() {
		if c.refreshStop != nil {
			close(c.refreshStop)
		}
	})

	if c.closed.Load() {
		return nil
	}
	c.closed.Store(true)
	c.single.Store(nil)
	c.fanout.close()

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

// publishSingleLocked caches the only member. Ejecting cannot select an
// alternative when the cluster has only one member.
func (c *Client) publishSingleLocked() {
	if len(c.addrs) == 1 {
		c.single.Store(c.addrs[0])
		return
	}
	c.single.Store(nil)
}

// snapshotCluster copies the current address list and picker under read lock.
func (c *Client) snapshotCluster() ([]*addr, picker) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.addrs, c.picker
}

// pickNode returns the node for key. One member skips routing and locking.
func (c *Client) pickNode(key []byte) (*addr, error) {
	if a := c.single.Load(); a != nil {
		return a, nil
	}
	addrs, picker := c.snapshotCluster()
	if picker == nil || len(addrs) == 0 {
		return nil, fmt.Errorf("no servers configured: %w", ErrInvalidAddress)
	}
	addrs = c.liveAddrs(addrs)
	if len(addrs) == 1 {
		return addrs[0], nil
	}
	addr, err := picker.Pick(addrs, key)
	if err != nil {
		return nil, fmt.Errorf("pick node failed: %w", err)
	}
	return addr, nil
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
	var first *addr
	for _, a := range addrs {
		if a == nil {
			continue
		}
		if first == nil {
			first = a
		}
		if !c.isDead(a) {
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
	if c == nil || c.rt == nil || len(c.rt.nsPrefix) == 0 {
		return key
	}
	if !bytes.HasPrefix(key, c.rt.nsPrefix) {
		return key
	}
	return key[len(c.rt.nsPrefix):]
}

var nsHdrPool = sync.Pool{
	New: func() any {
		s := make([][]byte, 0, 8)
		return &s
	},
}

func releaseWiredGetKeys(box *[]byte, hdrp *[][]byte) {
	putBuilderBox(box)
	if hdrp != nil {
		*hdrp = (*hdrp)[:0]
		nsHdrPool.Put(hdrp)
	}
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
	hdrp = nsHdrPool.Get().(*[][]byte)
	if cap(*hdrp) < n {
		*hdrp = make([][]byte, n)
	} else {
		*hdrp = (*hdrp)[:n]
	}
	hdr := *hdrp
	for i, key := range keys {
		start := len(buf)
		buf = append(buf, c.rt.nsPrefix...)
		buf = append(buf, key...)
		wiredKey := buf[start:]
		if err := validateKeyAndValue(wiredKey, nil); err != nil {
			putBuilderBox(box)
			*hdrp = (*hdrp)[:0]
			nsHdrPool.Put(hdrp)
			return nil, nil, nil, err
		}
		hdr[i] = wiredKey
	}
	*box = buf
	*hdrp = hdr
	return hdr, box, hdrp, nil
}
