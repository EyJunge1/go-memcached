package memcached

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

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
