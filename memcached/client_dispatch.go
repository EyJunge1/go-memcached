package memcached

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// getConn checks out a live socket from the node's pool.
func (c *Client) getConn(ctx context.Context, node *addr) (wireConn, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}
	if pool := node.pool.Load(); pool != nil {
		if !pool.closed.Load() {
			return c.checkoutPool(ctx, pool)
		}
		// Removed by SetServers/Close: drop the stale pointer and re-resolve membership.
		node.pool.CompareAndSwap(pool, nil)
	}

	key := node.id()

	pool, ok, err := func() (*connPool, bool, error) {
		c.mu.RLock()
		defer c.mu.RUnlock()
		if c.closed.Load() {
			return nil, false, ErrClosed
		}
		p, found := c.connPools[key]
		return p, found, nil
	}()
	if err != nil {
		return nil, err
	}
	if ok {
		node.pool.Store(pool)
		return c.checkoutPool(ctx, pool)
	}

	pool, created, err := func() (*connPool, bool, error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.closed.Load() {
			return nil, false, ErrClosed
		}
		if p, found := c.connPools[key]; found {
			return p, false, nil
		}
		member := false
		for _, a := range c.addrs {
			if a != nil && a.id() == key {
				member = true
				node = a
				break
			}
		}
		if !member {
			return nil, false, fmt.Errorf("server no longer configured: %w", ErrInvalidAddress)
		}

		server := addrToServer(node)
		wrapNewConn := func(ctx2 context.Context) (cn wireConn, err error) {
			start := time.Now()
			defer func() {
				if c.rt != nil && c.rt.onCreate != nil {
					c.rt.onCreate(PoolEvent{Server: server, Err: err, Duration: time.Since(start)})
				}
			}()
			switch node.Network {
			case
				"tcp", "tcp4", "tcp6",
				"unix",
				"udp", "udp4", "udp6":
			default:
				return nil, ErrInvalidNetworkProtocol
			}

			raw, dialErr := newConnContext(ctx2, node, c.rt)
			if dialErr != nil {
				err = dialErr
				return nil, fmt.Errorf("newConnContext: %w", err)
			}
			raw.now = resolveClock(c.now)
			cn = raw
			if (c.rt.authUser != "" || c.rt.authPass != "") && !isUDPNetwork(node.Network) {
				if err = validateAuthPeer(cn, node, c.rt); err != nil {
					_ = cn.Close()
					return nil, err
				}
				if err = validAuthSecret(c.rt.authUser); err != nil {
					_ = cn.Close()
					return nil, err
				}
				if err = validAuthSecret(c.rt.authPass); err != nil {
					_ = cn.Close()
					return nil, err
				}
				if err = authHandshake(
					ctx2, cn, c.rt.authUser, c.rt.authPass,
					c.rt.writeTimeout, c.rt.readTimeout,
				); err != nil {
					_ = cn.Close()
					return nil, err
				}
			}

			return cn, nil
		}

		var p *connPool
		if isUDPNetwork(node.Network) {
			p = newConnPool(1 /* maxIdle */, 1 /* maxOpen */, 0 /* maxLifetime */, 0 /* maxIdleTime */, wrapNewConn)
		} else {
			p = newConnPool(
				c.rt.maxIdle, c.rt.maxOpen,
				c.rt.maxLifetime, c.rt.maxIdleTime,
				wrapNewConn,
			)
			p.minIdle = c.rt.minIdle
		}
		p.server = server
		p.now = resolveClock(c.now)
		if c.rt != nil {
			p.onAcquire = c.rt.onAcquire
		}
		c.connPools[key] = p
		node.pool.Store(p)
		return p, true, nil
	}()
	if err != nil {
		return nil, err
	}
	// Unlock before warmup I/O (defer in the create scope above already released mu).
	if created {
		if !isUDPNetwork(node.Network) {
			pool.warmup(ctx, c.rt.minIdle)
		}
	} else {
		node.pool.Store(pool)
	}
	return c.checkoutPool(ctx, pool)
}

// checkoutPool leases from pool and maps a closed-client race to ErrClosed.
func (c *Client) checkoutPool(ctx context.Context, pool *connPool) (wireConn, error) {
	cn, err := pool.get(ctx)
	if err != nil && errors.Is(err, errPoolClosed) && c.closed.Load() {
		return nil, ErrClosed
	}
	return cn, err
}

// rotateUDPConnIfStale discards a pre-wrap UDP socket and checks out a fresh one.
// The request id must already be assigned so the wrap-request never reads on the old socket.
func (c *Client) rotateUDPConnIfStale(
	ctx context.Context,
	node *addr,
	cn wireConn,
) (wireConn, error) {
	if !udpConnEpochStale(cn) {
		return cn, nil
	}
	if p := cn.getConnPool(); p != nil {
		_ = p.discard(cn)
	} else {
		_ = cn.closeSilent()
	}
	fresh, err := c.getConn(ctx, node)
	if err != nil {
		return nil, wrapTransport(fmt.Errorf("alloc connection: %w", err))
	}
	if udpConnEpochStale(fresh) {
		if p := fresh.getConnPool(); p != nil {
			_ = p.discard(fresh)
		} else {
			_ = fresh.closeSilent()
		}
		return nil, wrapTransport(fmt.Errorf("udp epoch rotation: %w", ErrTransport))
	}
	return fresh, nil
}

// connHolder owns a checked-out wireConn so callers can replace it (e.g. UDP
// epoch rotation) without taking a pointer to the interface.
type connHolder struct {
	cn wireConn
}

// callFunc runs one request on an already checked-out connection. It receives
// a holder so UDP epoch rotation can replace the owned checkout safely.
type callFunc func(ctx context.Context, node *addr, conn *connHolder) error

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
		_ = cn.closeSilent()
		return
	}
	_ = cn.release()
}

// udpConnEpochStale reports whether a UDP socket predates the current request-id epoch.
func udpConnEpochStale(cn wireConn) bool {
	c, ok := cn.(*conn)
	if !ok || !isUDPNetwork(c.network) {
		return false
	}
	return c.udpEpoch != _udpRequestEpoch.Load()
}
