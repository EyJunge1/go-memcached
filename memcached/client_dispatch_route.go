package memcached

import (
	"context"
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"time"
)

const _retryBackoffCap = 250 * time.Millisecond

func (c *Client) dispatchFirstLive(ctx context.Context, req *request, resp *response) (err error) {
	obs := c.observer()
	var start time.Time
	var node *addr
	maxTries := max(c.rt.retryAttempts, 1)
	var tryLog []Attempt
	if obs != nil {
		tryLog = make([]Attempt, 0, maxTries)
		start = time.Now()
		defer func() {
			c.observeEvent(ctx, OpEvent{
				Op:           opName(req),
				Server:       serverFromAddr(node),
				Duration:     time.Since(start),
				Err:          err,
				Keys:         1,
				RequestBytes: requestWireBytes(req),
				Attempts:     tryLog,
			})
		}()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	var last error
	var prev *addr
	for i := 0; i < maxTries; i++ {
		node, err = c.pickFirstLiveAddr()
		if err != nil {
			return err
		}
		if node != nil && isUDPNetwork(node.Network) {
			maxTries = 1
		}
		if i > 0 {
			if err = c.maybeWaitRetry(ctx, prev, node, i); err != nil {
				return err
			}
			resp.rawLines = resp.rawLines[:0]
		}
		attemptStart := time.Now()
		attemptNode := node
		sent, err := c.dispatchOnce(ctx, node, req, resp)
		if obs != nil {
			tryLog = append(tryLog, Attempt{
				Server:   serverFromAddr(attemptNode),
				Duration: time.Since(attemptStart),
				Err:      err,
			})
		}
		if err == nil {
			c.noteSuccess(node)
			return nil
		}
		last = err
		prev = node
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
func (c *Client) dispatchToAddr(
	ctx context.Context,
	node *addr,
	req *request,
	resp *response,
) (err error) {
	observe := c.observer() != nil && !observeSkipped(ctx)
	var start time.Time
	maxTries := max(c.rt.retryAttempts, 1)
	if node != nil && isUDPNetwork(node.Network) {
		maxTries = 1
	}
	var tryLog []Attempt
	if observe {
		tryLog = make([]Attempt, 0, maxTries)
		start = time.Now()
		defer func() { c.observeDispatch(ctx, start, node, req, err, tryLog) }()
	}
	var last error
	var prev *addr
	for i := 0; i < maxTries; i++ {
		if i > 0 && c.rt.failover.Eject && c.isDead(node) {
			addrs, picker := c.snapshotCluster()
			addrs = c.liveAddrs(addrs)
			next, pickErr := picker.Pick(addrs, req.key)
			if pickErr == nil {
				node = next
			}
		}
		if i > 0 {
			if err = c.maybeWaitRetry(ctx, prev, node, i); err != nil {
				return err
			}
			resp.rawLines = resp.rawLines[:0]
		}
		attemptStart := time.Now()
		attemptNode := node
		sent, err := c.dispatchOnce(ctx, node, req, resp)
		if observe {
			tryLog = append(tryLog, Attempt{
				Server:   serverFromAddr(attemptNode),
				Duration: time.Since(attemptStart),
				Err:      err,
			})
		}
		if err == nil {
			c.noteSuccess(node)
			return nil
		}
		last = err
		prev = node
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

// maybeWaitRetry pauses before a same-node retry when RetryBackoff is set.
func (c *Client) maybeWaitRetry(ctx context.Context, prev, node *addr, attemptIndex int) error {
	if c == nil || c.rt == nil || c.rt.retryBackoff <= 0 || !sameNode(prev, node) {
		return nil
	}
	return waitRetry(ctx, retryDelay(c.rt.retryBackoff, attemptIndex))
}

// sameNode reports whether a and b identify the same cluster member.
func sameNode(a, b *addr) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.id() == b.id()
}

// retryDelay returns a full-jitter pause before retry attemptIndex (1 = before 2nd try).
// delay ∈ [0, min(base*2^(attemptIndex-1), 250ms)]. base <= 0 yields 0.
func retryDelay(base time.Duration, attemptIndex int) time.Duration {
	if base <= 0 || attemptIndex < 1 {
		return 0
	}
	bound := base
	for i := 1; i < attemptIndex && bound < _retryBackoffCap; i++ {
		bound *= 2
	}
	if bound > _retryBackoffCap {
		bound = _retryBackoffCap
	}
	if bound <= 0 {
		return 0
	}
	n, err := cryptorand.Int(cryptorand.Reader, big.NewInt(int64(bound)+1))
	if err != nil {
		return bound
	}
	return time.Duration(n.Int64())
}

// waitRetry sleeps for d or returns ctx.Err() if the context ends first.
func waitRetry(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// dispatchOnce checks out a conn, sends, and recvs. sent is true if the frame left the socket.
func (c *Client) dispatchOnce(
	ctx context.Context,
	node *addr,
	req *request,
	resp *response,
) (sent bool, err error) {
	if err = ctx.Err(); err != nil {
		return false, err
	}
	if c.streamCount() > 0 && node != nil && !isUDPNetwork(node.Network) {
		if err = c.dispatchViaStream(ctx, node, req, resp); err == nil {
			return true, nil
		}
		if !errors.Is(err, errNoStream) {
			return true, err
		}
	}

	cn, err := c.getConn(ctx, node)
	if err != nil {
		return false, wrapTransport(fmt.Errorf("alloc connection: %w", err))
	}
	defer func() { finishDispatch(cn, err) }()

	c.enableUDPFraming(node, req, resp)
	if req.udpEnabled {
		if err = udpRequestAllowed(req); err != nil {
			return false, err
		}
		// Allocate the id (and possible epoch bump) before I/O, then ensure the
		// socket matches the current epoch so a recycled id cannot match a late
		// datagram still queued on the previous UDP socket.
		cn, err = c.rotateUDPConnIfStale(ctx, node, cn)
		if err != nil {
			cn = nil
			return false, err
		}
	}
	if resp != nil {
		resp.maxValueBytes = c.valueLimit()
		resp.readTimeout = c.rt.readTimeout
		resp.readCtx = ctx
		resp.readConn = cn
	}

	stop := prepareOpIO(ctx, cn, c.rt.readTimeout, c.rt.writeTimeout)
	defer stop()
	if err = req.sendReady(ctx, cn); err != nil {
		err = wrapTransport(fmt.Errorf("send: %w", err))
		return false, err
	}
	sent = true
	if err = resp.recvReady(ctx, cn); err != nil {
		err = wrapRecvError(fmt.Errorf("recv: %w", err))
		return sent, err
	}
	if resp.afterRecv != nil {
		err = wrapRecvError(resp.afterRecv(resp))
		if err != nil {
			return sent, err
		}
	}
	if err = ensureNoTrailingBytes(cn); err != nil {
		err = wrapRecvError(err)
		return sent, err
	}
	return sent, nil
}

// valueLimit is the maximum VALUE/VA body the client will accept from the server.
func (c *Client) valueLimit() int {
	if c == nil || c.rt == nil || c.rt.maxValueBytes <= 0 {
		return _defaultMaxValueBytes
	}
	return c.rt.maxValueBytes
}

// dialDedicated opens an unpooled TCP or Unix socket for admin streams, with auth when configured.
func (c *Client) dialDedicated(ctx context.Context, node *addr) (wireConn, error) {
	if c.closed.Load() {
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
		return nil, fmt.Errorf("newConnContext: %w", err)
	}
	cn.now = resolveClock(c.now)
	if c.closed.Load() {
		_ = cn.Close()
		return nil, ErrClosed
	}
	if c.rt.authUser != "" || c.rt.authPass != "" {
		if err = validateAuthPeer(cn, node, c.rt); err != nil {
			_ = cn.Close()
			return nil, err
		}
		if err = authHandshake(
			ctx, cn, c.rt.authUser, c.rt.authPass,
			c.rt.writeTimeout, c.rt.readTimeout,
		); err != nil {
			_ = cn.Close()
			return nil, err
		}
	}
	if c.closed.Load() {
		_ = cn.Close()
		return nil, ErrClosed
	}
	return cn, nil
}

func finishDispatch(cn wireConn, err error) {
	finishConn(cn, err)
}
