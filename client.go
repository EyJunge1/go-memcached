package memcached

import (
	"bytes"
	"context"
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

// getConn returns a true connection from the pool.
func (c *Client) getConn(ctx context.Context, addr *Addr) (memcachedConn, error) {
	key := addr.id()

	c.mu.RLock()
	pool, ok := c.connPools[key]
	c.mu.RUnlock()
	if ok {
		return pool.get(ctx)
	}

	c.mu.Lock()
	pool, ok = c.connPools[key]
	if !ok {
		wrapNewConn := func(ctx2 context.Context) (cn memcachedConn, err error) {
			switch addr.Network {
			case
				"tcp", "tcp4", "tcp6",
				"unix",
				"udp", "udp4", "udp6":
			default:
				return nil, ErrInvalidNetworkProtocol
			}

			cn, err = newConnContext(ctx2, addr, c.options.dialTimeout, c.options.keepAlive, c.options.dialContext)
			if err != nil {
				return nil, fmt.Errorf("newConnContext failed: %w", err)
			}
			if (c.options.authUser != "" || c.options.authPass != "") && !isUDPNetwork(addr.Network) {
				if err = validAuthSecret(c.options.authUser); err != nil {
					_ = cn.Close()
					return nil, err
				}
				if err = validAuthSecret(c.options.authPass); err != nil {
					_ = cn.Close()
					return nil, err
				}
				if err = authenticateOnConn(ctx2, cn, c.options.authUser, c.options.authPass, c.options.writeTimeout, c.options.readTimeout); err != nil {
					_ = cn.Close()
					return nil, err
				}
			}

			return cn, nil
		}

		pool = newConnPool(
			c.options.maxIdleConns, c.options.maxConns,
			c.options.maxLifetime, c.options.maxIdleTimeout,
			wrapNewConn,
		)
		pool.minIdle = c.options.minIdleConns
		c.connPools[key] = pool
		c.mu.Unlock()
		pool.warmup(ctx, c.options.minIdleConns)
		return pool.get(ctx)
	}
	c.mu.Unlock()

	return pool.get(ctx)
}

type callFunc func(ctx context.Context, addr *Addr, conn memcachedConn) error

type transportError struct {
	err error
}

func (e *transportError) Error() string { return e.err.Error() }

func (e *transportError) Unwrap() error { return e.err }

func wrapTransport(err error) error {
	if err == nil {
		return nil
	}
	return &transportError{err: err}
}

func isTransportError(err error) bool {
	var te *transportError
	return errors.As(err, &te)
}

func isKeepaliveProtocolError(err error) bool {
	return errors.Is(err, ErrNotFound) ||
		errors.Is(err, ErrExists) ||
		errors.Is(err, ErrNotStored) ||
		errors.Is(err, ErrClientError) ||
		errors.Is(err, ErrNonexistentCommand) ||
		errors.Is(err, ErrBusy) ||
		errors.Is(err, ErrBadClass) ||
		errors.Is(err, ErrNoSpare) ||
		errors.Is(err, ErrNotFull) ||
		errors.Is(err, ErrUnsafe) ||
		errors.Is(err, ErrSame)
}

func wrapRecvError(err error) error {
	if err == nil {
		return nil
	}
	if isKeepaliveProtocolError(err) {
		return err
	}
	return wrapTransport(err)
}

func shouldDiscardConn(err error) bool {
	if err == nil {
		return false
	}
	if isKeepaliveProtocolError(err) {
		return false
	}
	return isTransportError(err) ||
		errors.Is(err, ErrMalformedResponse) ||
		errors.Is(err, ErrServerError) ||
		errors.Is(err, ErrUnknownIndicator)
}

func finishConn(cn memcachedConn, err error) {
	if cn == nil {
		return
	}
	if shouldDiscardConn(err) {
		if p := cn.getConnPool(); p != nil {
			_ = p.discard(cn)
			return
		}
		_ = cn.Close()
		return
	}
	_ = cn.release()
}

func (c *Client) enableUDPFraming(addr *Addr, req *request, resp *response) {
	if addr == nil || !isUDPNetwork(addr.Network) {
		req.udpEnabled = false
		resp.udpEnabled = false
		return
	}
	id := nextUDPRequestID()
	req.udpEnabled = true
	req.udpReqID = id
	resp.udpEnabled = true
	resp.udpReqID = id
}

func (c *Client) broadcastRequest(ctx context.Context, call callFunc) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	addrs, _ := c.snapshotCluster()

	var wg sync.WaitGroup
	errCh := make(chan error, len(addrs))

	for _, addr := range addrs {
		wg.Go(func() {
			cn, err := c.getConn(ctx, addr)
			if err != nil {
				errCh <- err
				return
			}

			var opErr error
			defer func() { finishConn(cn, opErr) }()

			opErr = call(ctx, addr, cn)
			if opErr != nil {
				errCh <- opErr
			}
		})
	}

	wg.Wait()
	close(errCh)

	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}

func (c *Client) dispatchRequest(ctx context.Context, req *request, resp *response) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	addrs, picker := c.snapshotCluster()
	addrs = c.liveAddrs(addrs)
	addr, err := picker.Pick(addrs, req.cmd, c.hashKey(req.key))
	if err != nil {
		return fmt.Errorf("pick node failed: %w", err)
	}
	return c.dispatchToAddr(ctx, addr, req, resp)
}

func (c *Client) dispatchToAddr(ctx context.Context, addr *Addr, req *request, resp *response) error {
	attempts := c.options.retryAttempts
	if attempts < 1 {
		attempts = 1
	}
	var last error
	for i := 0; i < attempts; i++ {
		if i > 0 && c.options.failover.EjectFromHash && c.isDead(addr) {
			addrs, picker := c.snapshotCluster()
			addrs = c.liveAddrs(addrs)
			next, pickErr := picker.Pick(addrs, req.cmd, c.hashKey(req.key))
			if pickErr == nil {
				addr = next
			}
		}
		if i > 0 {
			resp.rawLines = resp.rawLines[:0]
		}
		err := c.dispatchOnce(ctx, addr, req, resp)
		if err == nil {
			c.noteSuccess(addr)
			return nil
		}
		last = err
		if !isRetryable(err) {
			return err
		}
		c.noteFailure(addr)
	}
	return last
}

func (c *Client) dispatchOnce(ctx context.Context, addr *Addr, req *request, resp *response) (err error) {
	if err = ctx.Err(); err != nil {
		return err
	}

	cn, err := c.getConn(ctx, addr)
	if err != nil {
		return wrapTransport(fmt.Errorf("alloc connection failed: %w", err))
	}
	defer func() { finishConn(cn, err) }()

	c.enableUDPFraming(addr, req, resp)

	if err = req.send(ctx, cn, c.options.writeTimeout); err != nil {
		err = wrapTransport(fmt.Errorf("send failed: %w", err))
		return err
	}
	if err = resp.recv(ctx, cn, c.options.readTimeout); err != nil {
		err = wrapRecvError(fmt.Errorf("recv failed: %w", err))
		return err
	}
	if resp.afterRecv != nil {
		err = wrapRecvError(resp.afterRecv(resp))
	}
	return err
}

func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	if isKeepaliveProtocolError(err) ||
		errors.Is(err, ErrMalformedResponse) ||
		errors.Is(err, ErrUnknownIndicator) {
		return false
	}
	return isTransportError(err)
}

func (c *Client) liveAddrs(addrs []*Addr) []*Addr {
	if !c.options.failover.EjectFromHash || c.options.failover.FailureLimit <= 0 {
		return addrs
	}
	live := make([]*Addr, 0, len(addrs))
	for _, a := range addrs {
		if !c.isDead(a) {
			live = append(live, a)
		}
	}
	if len(live) == 0 {
		return addrs
	}
	return live
}

func (c *Client) isDead(addr *Addr) bool {
	if addr == nil || c.options.failover.FailureLimit <= 0 {
		return false
	}
	c.healthMu.Lock()
	defer c.healthMu.Unlock()
	until, ok := c.deadUntil[addr.id()]
	return ok && nowFunc().Before(until)
}

func (c *Client) noteFailure(addr *Addr) {
	if addr == nil || c.options.failover.FailureLimit <= 0 {
		return
	}
	c.healthMu.Lock()
	defer c.healthMu.Unlock()
	id := addr.id()
	c.failCount[id]++
	if c.failCount[id] >= c.options.failover.FailureLimit {
		c.deadUntil[id] = nowFunc().Add(c.options.failover.RetryTimeout)
	}
}

func (c *Client) noteSuccess(addr *Addr) {
	if addr == nil {
		return
	}
	c.healthMu.Lock()
	defer c.healthMu.Unlock()
	id := addr.id()
	delete(c.failCount, id)
	delete(c.deadUntil, id)
}

// DeadServers returns address IDs currently in failover quarantine.
func (c *Client) DeadServers() []string {
	c.healthMu.Lock()
	defer c.healthMu.Unlock()
	now := nowFunc()
	var out []string
	for id, until := range c.deadUntil {
		if now.Before(until) {
			out = append(out, id)
		}
	}
	return out
}

func (c *Client) startResolverRefresh() {
	if c.options == nil || c.options.resolverRefresh <= 0 {
		return
	}
	c.refreshStop = make(chan struct{})
	go func() {
		t := time.NewTicker(c.options.resolverRefresh)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				c.mu.RLock()
				raw := c.addrString
				c.mu.RUnlock()
				_ = c.SetServers(raw)
			case <-c.refreshStop:
				return
			}
		}
	}()
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

func authenticateOnConn(ctx context.Context, cn memcachedConn, user, pass string, writeTimeout, readTimeout time.Duration) error {
	req, resp := buildAuthCommand(user, pass)
	defer releaseReqAndResp(req, resp)
	resp.afterRecv = mapStorageReply
	if err := req.send(ctx, cn, writeTimeout); err != nil {
		return wrapTransport(fmt.Errorf("auth send failed: %w", err))
	}
	if err := resp.recv(ctx, cn, readTimeout); err != nil {
		return wrapRecvError(fmt.Errorf("auth recv failed: %w", err))
	}
	if resp.afterRecv != nil {
		return wrapRecvError(resp.afterRecv(resp))
	}
	return nil
}
