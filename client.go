package memcached

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Client is a thread-safe memcached client. It manages a connection pool per
// server address and, in cluster mode, picks a node per key.
type Client struct {
	options *clientOptions

	mu        sync.RWMutex
	addrs     []*Addr
	picker    Picker
	connPools map[string]*connPool
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

	addrs, err := options.resolver.Resolve(addr)
	if err != nil {
		return nil, fmt.Errorf("resolve failed: %w", err)
	}

	if len(addrs) == 0 {
		return nil, fmt.Errorf("empty address: %w", ErrInvalidAddress)
	}

	return &Client{
		options:   options,
		addrs:     addrs,
		picker:    options.pickBuilder.Build(addrs),
		connPools: make(map[string]*connPool, 4),
	}, nil
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
	return nil
}

func (c *Client) Close() error {
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

			cn, err = newConnContext(ctx2, addr, c.options.dialTimeout, c.options.dialContext)
			if err != nil {
				return nil, fmt.Errorf("newConnContext failed: %w", err)
			}

			return cn, nil
		}

		pool = newConnPool(
			c.options.maxIdleConns, c.options.maxConns,
			c.options.maxLifetime, c.options.maxIdleTimeout,
			wrapNewConn,
		)
		c.connPools[key] = pool
	}
	c.mu.Unlock()

	return pool.get(ctx)
}

type callFunc func(ctx context.Context, conn memcachedConn) error

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

func finishConn(cn memcachedConn, err error) {
	if cn == nil {
		return
	}
	if isTransportError(err) {
		if p := cn.getConnPool(); p != nil {
			_ = p.discard(cn)
			return
		}
		_ = cn.Close()
		return
	}
	_ = cn.release()
}

func (c *Client) autoSwitchToUDP(_ context.Context, req *request, resp *response) {
	req.udpEnabled = c.options.enableUDP
	resp.udpEnabled = c.options.enableUDP
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

			opErr = call(ctx, cn)
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
	addr, err := picker.Pick(addrs, req.cmd, req.key)
	if err != nil {
		return fmt.Errorf("pick node failed: %w", err)
	}
	return c.dispatchToAddr(ctx, addr, req, resp)
}

func (c *Client) dispatchToAddr(ctx context.Context, addr *Addr, req *request, resp *response) (err error) {
	if err = ctx.Err(); err != nil {
		return err
	}

	cn, err := c.getConn(ctx, addr)
	if err != nil {
		return fmt.Errorf("alloc connection failed: %w", err)
	}
	defer func() { finishConn(cn, err) }()

	c.autoSwitchToUDP(ctx, req, resp)

	if err = req.send(ctx, cn, c.options.writeTimeout); err != nil {
		err = wrapTransport(fmt.Errorf("send failed: %w", err))
		return err
	}
	if err = resp.recv(ctx, cn, c.options.readTimeout); err != nil {
		err = wrapTransport(fmt.Errorf("recv failed: %w", err))
		return err
	}
	return nil
}
