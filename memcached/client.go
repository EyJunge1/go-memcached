package memcached

import (
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
	refreshWG   sync.WaitGroup
	healthStop  chan struct{}
	healthOnce  sync.Once
	healthWG    sync.WaitGroup
	closeMu     sync.Mutex

	resolverMu  sync.Mutex
	resolverErr error

	closed         atomic.Bool
	single         atomic.Pointer[addr]
	clusterVersion atomic.Uint64
	healthVersion  atomic.Uint64
	meta           MetaClient
	fanout         *fanoutPool

	streams  map[string][]*sharedStream
	streamRR atomic.Uint64
	// opaqueSeq allocates unique O tokens across SharedStreams meta mux
	// (single-ops and Meta().Pipeline waves share one namespace).
	opaqueSeq atomic.Uint64

	now clockFunc
}

// New builds a Client from Config.
func New(cfg Config) (*Client, error) {
	// Copy slice/pointer config inputs so caller mutations after New cannot
	// race with validation or runtime setup (Uber: copy at boundaries).
	if n := len(cfg.Servers); n > 0 {
		servers := make([]Server, n)
		copy(servers, cfg.Servers)
		cfg.Servers = servers
	}
	if cfg.Auth != nil {
		auth := *cfg.Auth
		cfg.Auth = &auth
	}
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
		connPools:        make(map[string]*connPool, len(addrs)),
		resolveSpec:      resolveSpec,
		resolveSpecFixed: resolveSpecFixed,
		failCount:        make(map[string]int, len(addrs)),
		deadUntil:        make(map[string]time.Time, len(addrs)),
		now:              time.Now,
	}
	c.meta.c = c
	c.fanout = newFanoutPool(rt.fanoutWorkers)
	c.clusterVersion.Store(1)
	c.publishSingleLocked()
	c.startResolverRefresh()
	c.startIdleHealthPing()
	// Memorystore: resolve once before returning so membership is not stuck
	// on the seed until the first ticker tick. Failures keep the seed and
	// surface via ResolverErr (do not fail New).
	if rt.memorystoreDiscovery {
		c.refreshMembership(true /* fromTicker */)
	}
	return c, nil
}

// Close stops resolver refresh, cancels persistent fanout work, and closes idle
// pooled sockets (TCP/Unix send quit first; UDP does not).
//
// Close does not cancel already checked-out caller work that uses
// context.Background() together with TimeoutNone. This includes single-node
// calls, ephemeral fanout (FanoutWorkers == 0), broadcasts, Sessions, and
// dedicated admin streams. Such work ends when the peer responds or its caller
// context is canceled. Callers must still close Sessions. After Close, new API
// calls return ErrClosed.
func (c *Client) Close() error {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	if c.rt != nil {
		if ms, ok := c.rt.resolver.(*memorystoreResolver); ok {
			ms.closeConn()
		}
	}
	c.refreshOnce.Do(func() {
		if c.refreshStop != nil {
			close(c.refreshStop)
		}
	})
	c.healthOnce.Do(func() {
		if c.healthStop != nil {
			close(c.healthStop)
		}
	})
	c.refreshWG.Wait()
	c.healthWG.Wait()

	firstClose := !c.closed.Swap(true)
	if firstClose {
		c.single.Store(nil)
		c.fanout.closeFanout()
	}
	// Always drain streams: a racing post-close op must not leave loop/reader
	// goroutines behind once Close returns (or is called again from Cleanup).
	c.closeStreams()
	if !firstClose {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	errs := make([]error, 0, len(c.connPools))
	for _, pool := range c.connPools {
		if err := pool.closePool(); err != nil {
			errs = append(errs, err)
		}
	}
	c.connPools = nil
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

// snapshotCluster returns the current address list and picker under read lock.
// The slice aliases Client membership; callers must treat it as immutable.
// Membership swaps replace the slice rather than mutate it in place.
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
		return nil, fmt.Errorf("pick node: %w", err)
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

// pickFirstLiveAddr returns the first live member, or the first if all are dead.
// An empty cluster is ErrInvalidAddress.
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
	defer c.resolverMu.Unlock()
	c.resolverErr = err
}

// serverKey identifies a Server by Network and Address for membership maps.
func serverKey(s Server) string {
	return serverNetwork(s) + "\x00" + s.Address
}
