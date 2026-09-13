package memcached

import (
	"context"
	"errors"
	"fmt"
	"sync"
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

			cn, err = newConnContext(ctx2, addr, c.options)
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
				if err = authHandshake(ctx2, cn, c.options.authUser, c.options.authPass, c.options.writeTimeout, c.options.readTimeout); err != nil {
					_ = cn.Close()
					return nil, err
				}
			}

			return cn, nil
		}

		if isUDPNetwork(addr.Network) {
			pool = newConnPool(1, 1, 0, 0, wrapNewConn)
		} else {
			pool = newConnPool(
				c.options.maxIdleConns, c.options.maxConns,
				c.options.maxLifetime, c.options.maxIdleTimeout,
				wrapNewConn,
			)
			pool.minIdle = c.options.minIdleConns
		}
		c.connPools[key] = pool
		c.mu.Unlock()
		if !isUDPNetwork(addr.Network) {
			pool.warmup(ctx, c.options.minIdleConns)
		}
		return pool.get(ctx)
	}
	c.mu.Unlock()

	return pool.get(ctx)
}

type callFunc func(ctx context.Context, addr *Addr, conn memcachedConn) error

func wrapTransport(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrTransport) {
		return err
	}
	if isClassifiedNonTransport(err) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrTransport, err)
}

func isClassifiedNonTransport(err error) bool {
	return isKeepaliveProtocolError(err) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, ErrServerError) ||
		errors.Is(err, ErrMalformedResponse) ||
		errors.Is(err, ErrUnknownIndicator) ||
		errors.Is(err, ErrInvalidArgument) ||
		errors.Is(err, ErrInvalidKey) ||
		errors.Is(err, ErrInvalidValue) ||
		errors.Is(err, ErrInvalidAddress) ||
		errors.Is(err, ErrInvalidNetworkProtocol) ||
		errors.Is(err, ErrNotSupported) ||
		errors.Is(err, ErrFlushAllNotAllowed) ||
		errors.Is(err, ErrAdminNotEnabled) ||
		errors.Is(err, ErrSetServersNotAllowed) ||
		errors.Is(err, ErrMixedTransport) ||
		errors.Is(err, ErrInsecureTLS) ||
		errors.Is(err, ErrDeadlineRequired) ||
		errors.Is(err, ErrAdminTargetRequired)
}

func isTransportError(err error) bool {
	return errors.Is(err, ErrTransport)
}

func isKeepaliveProtocolError(err error) bool {
	return errors.Is(err, ErrNotFound) ||
		errors.Is(err, ErrExists) ||
		errors.Is(err, ErrNotStored) ||
		errors.Is(err, ErrClientError) ||
		errors.Is(err, ErrNonexistentCommand) ||
		errors.Is(err, ErrCommandError) ||
		errors.Is(err, ErrBusy) ||
		errors.Is(err, ErrBadClass) ||
		errors.Is(err, ErrNoSpare) ||
		errors.Is(err, ErrNotFull) ||
		errors.Is(err, ErrUnsafe) ||
		errors.Is(err, ErrSame) ||
		errors.Is(err, ErrWatcherTooMany) ||
		errors.Is(err, ErrWatcherFailed) ||
		errors.Is(err, ErrMemlimitTooSmall) ||
		errors.Is(err, ErrMemlimitAdjustFailed) ||
		errors.Is(err, ErrNotStarted)
}

func wrapRecvError(err error) error {
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

func (c *Client) sendOnConn(ctx context.Context, addr *Addr, cn memcachedConn, req *request, resp *response) error {
	c.enableUDPFraming(addr, req, resp)
	if req.udpEnabled {
		if err := udpRequestAllowed(req); err != nil {
			return err
		}
	}
	if resp != nil {
		resp.maxValueBytes = c.valueLimit()
		resp.readTimeout = c.options.readTimeout
		resp.readCtx = ctx
		resp.readConn = cn
	}
	if err := req.send(ctx, cn, c.options.writeTimeout); err != nil {
		return wrapTransport(fmt.Errorf("send failed: %w", err))
	}
	if err := resp.recv(ctx, cn, c.options.readTimeout); err != nil {
		return wrapRecvError(fmt.Errorf("recv failed: %w", err))
	}
	return nil
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
				c.noteFailure(addr)
				errCh <- err
				return
			}

			var opErr error
			defer func() { finishConn(cn, opErr) }()

			opErr = call(ctx, addr, cn)
			if opErr != nil {
				c.noteFailure(addr)
				errCh <- opErr
				return
			}
			c.noteSuccess(addr)
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
	addr, err := picker.Pick(addrs, c.hashKey(req.key))
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
	if addr != nil && isUDPNetwork(addr.Network) {
		attempts = 1
	}
	var last error
	for i := 0; i < attempts; i++ {
		if i > 0 && c.options.failover.Eject && c.isDead(addr) {
			addrs, picker := c.snapshotCluster()
			addrs = c.liveAddrs(addrs)
			next, pickErr := picker.Pick(addrs, c.hashKey(req.key))
			if pickErr == nil {
				addr = next
			}
		}
		if i > 0 {
			resp.rawLines = resp.rawLines[:0]
		}
		sent, err := c.dispatchOnce(ctx, addr, req, resp)
		if err == nil {
			c.noteSuccess(addr)
			return nil
		}
		last = err
		if !isRetryable(err) {
			return err
		}
		c.noteFailure(addr)
		if sent && !idempotentReadCommand(req.cmd) {
			return err
		}
	}
	return last
}

func (c *Client) dispatchOnce(ctx context.Context, addr *Addr, req *request, resp *response) (sent bool, err error) {
	if err = ctx.Err(); err != nil {
		return false, err
	}

	cn, err := c.getConn(ctx, addr)
	if err != nil {
		return false, wrapTransport(fmt.Errorf("alloc connection failed: %w", err))
	}
	defer func() { finishConn(cn, err) }()

	c.enableUDPFraming(addr, req, resp)
	if req.udpEnabled {
		if err = udpRequestAllowed(req); err != nil {
			return false, err
		}
	}
	if resp != nil {
		resp.maxValueBytes = c.valueLimit()
		resp.readTimeout = c.options.readTimeout
		resp.readCtx = ctx
		resp.readConn = cn
	}

	if err = req.send(ctx, cn, c.options.writeTimeout); err != nil {
		err = wrapTransport(fmt.Errorf("send failed: %w", err))
		return false, err
	}
	sent = true
	if err = resp.recv(ctx, cn, c.options.readTimeout); err != nil {
		err = wrapRecvError(fmt.Errorf("recv failed: %w", err))
		return sent, err
	}
	if resp.afterRecv != nil {
		err = wrapRecvError(resp.afterRecv(resp))
	}
	return sent, err
}

func (c *Client) valueLimit() int {
	if c == nil || c.options == nil || c.options.maxValueBytes <= 0 {
		return defaultMaxValueBytes
	}
	return c.options.maxValueBytes
}

func (c *Client) dialDedicated(ctx context.Context, addr *Addr) (memcachedConn, error) {
	if addr == nil {
		return nil, fmt.Errorf("no available address: %w", ErrInvalidAddress)
	}
	if isUDPNetwork(addr.Network) {
		return nil, fmt.Errorf("admin streams: %w", ErrNotSupported)
	}
	cn, err := newConnContext(ctx, addr, c.options)
	if err != nil {
		return nil, fmt.Errorf("newConnContext failed: %w", err)
	}
	if c.options.authUser != "" || c.options.authPass != "" {
		if err = authHandshake(ctx, cn, c.options.authUser, c.options.authPass, c.options.writeTimeout, c.options.readTimeout); err != nil {
			_ = cn.Close()
			return nil, err
		}
	}
	return cn, nil
}
