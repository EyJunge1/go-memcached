package memcached

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// getConn checks out a live socket from the node's pool.
func (c *Client) getConn(ctx context.Context, node *addr) (wireConn, error) {
	key := node.id()

	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, ErrClosed
	}
	pool, ok := c.connPools[key]
	c.mu.RUnlock()
	if ok {
		return pool.get(ctx)
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrClosed
	}
	pool, ok = c.connPools[key]
	if !ok {
		wrapNewConn := func(ctx2 context.Context) (cn wireConn, err error) {
			switch node.Network {
			case
				"tcp", "tcp4", "tcp6",
				"unix",
				"udp", "udp4", "udp6":
			default:
				return nil, ErrInvalidNetworkProtocol
			}

			cn, err = newConnContext(ctx2, node, c.rt)
			if err != nil {
				return nil, fmt.Errorf("newConnContext failed: %w", err)
			}
			if (c.rt.authUser != "" || c.rt.authPass != "") && !isUDPNetwork(node.Network) {
				if err = validAuthSecret(c.rt.authUser); err != nil {
					_ = cn.Close()
					return nil, err
				}
				if err = validAuthSecret(c.rt.authPass); err != nil {
					_ = cn.Close()
					return nil, err
				}
				if err = authHandshake(ctx2, cn, c.rt.authUser, c.rt.authPass, c.rt.writeTimeout, c.rt.readTimeout); err != nil {
					_ = cn.Close()
					return nil, err
				}
			}

			return cn, nil
		}

		if isUDPNetwork(node.Network) {
			pool = newConnPool(1, 1, 0, 0, wrapNewConn)
		} else {
			pool = newConnPool(
				c.rt.maxIdle, c.rt.maxOpen,
				c.rt.maxLifetime, c.rt.maxIdleTime,
				wrapNewConn,
			)
			pool.minIdle = c.rt.minIdle
		}
		c.connPools[key] = pool
		c.mu.Unlock()
		if !isUDPNetwork(node.Network) {
			pool.warmup(ctx, c.rt.minIdle)
		}
		return pool.get(ctx)
	}
	c.mu.Unlock()

	return pool.get(ctx)
}

// callFunc runs one request on an already checked-out connection.
type callFunc func(ctx context.Context, node *addr, conn wireConn) error

// wrapTransport marks unclassified I/O failures as ErrTransport.
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

// isClassifiedNonTransport is true for protocol, validation, and context errors.
func isClassifiedNonTransport(err error) bool {
	return isKeepaliveProtocolError(err) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, ErrServerError) ||
		errors.Is(err, ErrMalformedResponse) ||
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
		errors.Is(err, ErrAdminTargetRequired) ||
		errors.Is(err, ErrClosed)
}

// isTransportError reports whether err is or wraps ErrTransport.
func isTransportError(err error) bool {
	return errors.Is(err, ErrTransport)
}

// isKeepaliveProtocolError reports protocol replies that leave the connection reusable.
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

// wrapRecvError classifies a receive-path failure the same way as wrapTransport.
func wrapRecvError(err error) error {
	return wrapTransport(err)
}

// shouldDiscardConn reports whether err means the pooled socket can no longer be reused.
func shouldDiscardConn(err error) bool {
	if err == nil {
		return false
	}
	if isKeepaliveProtocolError(err) {
		return false
	}
	return isTransportError(err) ||
		errors.Is(err, ErrMalformedResponse) ||
		errors.Is(err, ErrServerError)
}

// finishConn returns cn to its pool, or closes it when shouldDiscardConn is true.
func finishConn(cn wireConn, err error) {
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

// sendOnConn writes req on an already-checked-out connection and reads resp.
func (c *Client) sendOnConn(ctx context.Context, node *addr, cn wireConn, req *request, resp *response) error {
	c.enableUDPFraming(node, req, resp)
	if req.udpEnabled {
		if err := udpRequestAllowed(req); err != nil {
			return err
		}
	}
	if resp != nil {
		resp.maxValueBytes = c.valueLimit()
		resp.readTimeout = c.rt.readTimeout
		resp.readCtx = ctx
		resp.readConn = cn
	}
	if err := req.send(ctx, cn, c.rt.writeTimeout); err != nil {
		return wrapTransport(fmt.Errorf("send failed: %w", err))
	}
	if err := resp.recv(ctx, cn, c.rt.readTimeout); err != nil {
		return wrapRecvError(fmt.Errorf("recv failed: %w", err))
	}
	return nil
}

// enableUDPFraming attaches a shared request id to req and resp when node uses UDP.
func (c *Client) enableUDPFraming(node *addr, req *request, resp *response) {
	if node == nil || !isUDPNetwork(node.Network) {
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

// broadcastRequest runs call on every configured node and joins the resulting errors.
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

// dispatchRequest picks a live cluster member for req.key and runs the request there.
func (c *Client) dispatchRequest(ctx context.Context, req *request, resp *response) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	addrs, picker := c.snapshotCluster()
	addrs = c.liveAddrs(addrs)
	addr, err := picker.Pick(addrs, req.key)
	if err != nil {
		return fmt.Errorf("pick node failed: %w", err)
	}
	return c.dispatchToAddr(ctx, addr, req, resp)
}

// dispatchFirstLive writes to the first live node and picks again on each
// retry. It does not use Ketama.
func (c *Client) dispatchFirstLive(ctx context.Context, req *request, resp *response) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	attempts := c.rt.retryAttempts
	if attempts < 1 {
		attempts = 1
	}
	var last error
	for i := 0; i < attempts; i++ {
		node, err := c.pickFirstLiveAddr()
		if err != nil {
			return err
		}
		if node != nil && isUDPNetwork(node.Network) {
			attempts = 1
		}
		if i > 0 {
			resp.rawLines = resp.rawLines[:0]
		}
		sent, err := c.dispatchOnce(ctx, node, req, resp)
		if err == nil {
			c.noteSuccess(node)
			return nil
		}
		last = err
		if !isRetryable(err) {
			return err
		}
		c.noteFailure(node)
		if sent && !idempotentReadCommand(req.cmd) {
			return err
		}
	}
	return last
}

// dispatchToAddr sends req to node, retrying transport failures according to Config.
func (c *Client) dispatchToAddr(ctx context.Context, node *addr, req *request, resp *response) error {
	attempts := c.rt.retryAttempts
	if attempts < 1 {
		attempts = 1
	}
	if node != nil && isUDPNetwork(node.Network) {
		attempts = 1
	}
	var last error
	for i := 0; i < attempts; i++ {
		if i > 0 && c.rt.failover.Eject && c.isDead(node) {
			addrs, picker := c.snapshotCluster()
			addrs = c.liveAddrs(addrs)
			next, pickErr := picker.Pick(addrs, req.key)
			if pickErr == nil {
				node = next
			}
		}
		if i > 0 {
			resp.rawLines = resp.rawLines[:0]
		}
		sent, err := c.dispatchOnce(ctx, node, req, resp)
		if err == nil {
			c.noteSuccess(node)
			return nil
		}
		last = err
		if !isRetryable(err) {
			return err
		}
		c.noteFailure(node)
		if sent && !idempotentReadCommand(req.cmd) {
			return err
		}
	}
	return last
}

// dispatchOnce checks out a conn, sends, and recvs. sent is true if the frame left the socket.
func (c *Client) dispatchOnce(ctx context.Context, node *addr, req *request, resp *response) (sent bool, err error) {
	if err = ctx.Err(); err != nil {
		return false, err
	}

	cn, err := c.getConn(ctx, node)
	if err != nil {
		return false, wrapTransport(fmt.Errorf("alloc connection failed: %w", err))
	}
	defer func() { finishConn(cn, err) }()

	c.enableUDPFraming(node, req, resp)
	if req.udpEnabled {
		if err = udpRequestAllowed(req); err != nil {
			return false, err
		}
	}
	if resp != nil {
		resp.maxValueBytes = c.valueLimit()
		resp.readTimeout = c.rt.readTimeout
		resp.readCtx = ctx
		resp.readConn = cn
	}

	if err = req.send(ctx, cn, c.rt.writeTimeout); err != nil {
		err = wrapTransport(fmt.Errorf("send failed: %w", err))
		return false, err
	}
	sent = true
	if err = resp.recv(ctx, cn, c.rt.readTimeout); err != nil {
		err = wrapRecvError(fmt.Errorf("recv failed: %w", err))
		return sent, err
	}
	if resp.afterRecv != nil {
		err = wrapRecvError(resp.afterRecv(resp))
	}
	return sent, err
}

// valueLimit is the maximum VALUE/VA body the client will accept from the server.
func (c *Client) valueLimit() int {
	if c == nil || c.rt == nil || c.rt.maxValueBytes <= 0 {
		return defaultMaxValueBytes
	}
	return c.rt.maxValueBytes
}

// dialDedicated opens an unpooled TCP or Unix socket for admin streams, with auth when configured.
func (c *Client) dialDedicated(ctx context.Context, node *addr) (wireConn, error) {
	c.mu.RLock()
	closed := c.closed
	c.mu.RUnlock()
	if closed {
		return nil, ErrClosed
	}
	if node == nil {
		return nil, fmt.Errorf("no available address: %w", ErrInvalidAddress)
	}
	if isUDPNetwork(node.Network) {
		return nil, fmt.Errorf("admin streams: %w", ErrNotSupported)
	}
	cn, err := newConnContext(ctx, node, c.rt)
	if err != nil {
		return nil, fmt.Errorf("newConnContext failed: %w", err)
	}
	if c.rt.authUser != "" || c.rt.authPass != "" {
		if err = authHandshake(ctx, cn, c.rt.authUser, c.rt.authPass, c.rt.writeTimeout, c.rt.readTimeout); err != nil {
			_ = cn.Close()
			return nil, err
		}
	}
	return cn, nil
}
