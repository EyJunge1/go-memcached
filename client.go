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

	// addrs represents the list of memcached addresses.
	// each one of them means a memcached server instance.
	addrs []*Addr

	// picker represents the picker strategy.
	// it is used to pick a memcached server instance to execute a command.
	picker Picker

	mu        sync.Mutex // guards following
	connPools map[*Addr]*connPool
}

// New creates a memcached client for the given address and options.
//
// The address may be a single node or a comma-separated cluster list.
// The same key is always sent to the same instance. Commands that take
// multiple keys (gets, gats) do not follow that rule unless all keys
// live on one instance.
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
		connPools: make(map[*Addr]*connPool, 4),
	}, nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, pool := range c.connPools {
		if err := pool.close(); err != nil {
			return fmt.Errorf("Close: %w", err)
		}
	}

	return nil
}

// getConn returns a true connection from the pool.
func (c *Client) getConn(ctx context.Context, addr *Addr) (memcachedConn, error) {
	c.mu.Lock()
	pool, ok := c.connPools[addr]
	if ok {
		c.mu.Unlock()
		cn, err := pool.get(ctx)
		return cn, err
	}

	wrapNewConn := func(ctx2 context.Context) (cn memcachedConn, err error) {
		switch addr.Network {
		case
			"tcp", "tcp4", "tcp6",
			"unix",
			"udp", "udp4", "udp6":
		default:
			return nil, ErrInvalidNetworkProtocol
		}

		cn, err = newConnContext(ctx2, addr, c.options.dialTimeout)
		if err != nil {
			return nil, fmt.Errorf("newConnContext failed: %w", err)
		}

		return cn, nil
	}

	// could not find a pool for the given addr, create a new one
	pool = newConnPool(
		c.options.maxIdleConns, c.options.maxConns,
		c.options.maxLifetime, c.options.maxIdleTimeout,
		wrapNewConn,
	)
	c.connPools[addr] = pool
	c.mu.Unlock()

	cn, err := pool.get(ctx)
	return cn, err
}

type callFunc func(ctx context.Context, conn memcachedConn) error

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

	var wg sync.WaitGroup

	errCh := make(chan error, len(c.addrs))

	for _, addr := range c.addrs {
		wg.Go(func() {
			cn, err := c.getConn(ctx, addr)
			if err != nil {
				errCh <- err
				return
			}
			defer func() { _ = cn.release() }()

			if err = call(ctx, cn); err != nil {
				errCh <- err
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

	addr, err := c.picker.Pick(c.addrs, req.cmd, req.key)
	if err != nil {
		return fmt.Errorf("pick node failed: %w", err)
	}

	cn, err := c.getConn(ctx, addr)
	if err != nil {
		return fmt.Errorf("alloc connection failed: %w", err)
	}
	defer func() { _ = cn.release() }()

	c.autoSwitchToUDP(ctx, req, resp)

	if err = req.send(ctx, cn, c.options.writeTimeout); err != nil {
		return fmt.Errorf("send failed: %w", err)
	}

	return resp.recv(ctx, cn, c.options.readTimeout)
}
