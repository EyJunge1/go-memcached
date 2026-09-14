package memcached

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// FlushOpts sets flush_all delay, cluster stagger, and noreply. Nil Delay runs now and may stagger as Rel(i*Stagger); ExptimeImmediate sends -1.
// Quiet is text noreply: the connection is discarded after send; CLIENT_ERROR and
// SERVER_ERROR are still returned when the server already replied.
// Stagger applies only to Admin().All().Flush.
type FlushOpts struct {
	Delay   *Exptime
	Stagger time.Duration
	Quiet   bool
}

// flushDelayFor returns the flush_all delay for node index i from FlushOpts.
func flushDelayFor(o FlushOpts) func(int) Exptime {
	return func(i int) Exptime {
		if o.Delay != nil {
			return *o.Delay
		}
		return Rel(time.Duration(i) * o.Stagger)
	}
}

// flushOptsValid rejects a negative Stagger or Delay combined with Stagger.
func flushOptsValid(o FlushOpts) error {
	if o.Stagger < 0 {
		return fmt.Errorf("flush_all: %w", ErrInvalidArgument)
	}
	if o.Delay != nil && o.Stagger != 0 {
		return fmt.Errorf("flush_all Delay excludes Stagger: %w", ErrInvalidArgument)
	}
	return nil
}

// Flush sends flush_all on the On() node. Config.AllowFlushAll must be set.
func (a *AdminNode) Flush(ctx context.Context, o FlushOpts) error {
	if a == nil || a.c == nil {
		return ErrAdminNotEnabled
	}
	if err := flushOptsValid(o); err != nil {
		return err
	}
	addr, err := a.resolveTarget()
	if err != nil {
		return err
	}
	if !a.c.rt.allowFlushAll {
		return ErrFlushAllNotAllowed
	}
	return a.c.flushAllOnAddr(ctx, addr, flushDelayFor(o)(0), o.Quiet)
}

// Flush sends flush_all to every configured server.
func (b *AdminBroadcast) Flush(ctx context.Context, o FlushOpts) error {
	if b == nil || b.c == nil {
		return ErrAdminNotEnabled
	}
	if err := flushOptsValid(o); err != nil {
		return err
	}
	return b.c.flushAllBroadcast(ctx, flushDelayFor(o), o.Quiet)
}

// flushAllBroadcast sends flush_all to every configured node.
func (c *Client) flushAllBroadcast(ctx context.Context, delayFor func(int) Exptime, quiet bool) error {
	if !c.rt.allowFlushAll {
		return ErrFlushAllNotAllowed
	}

	addrs, _ := c.snapshotCluster()
	if len(addrs) == 1 {
		return c.flushAllOnAddr(ctx, addrs[0], delayFor(0), quiet)
	}
	var wg sync.WaitGroup
	errCh := make(chan error, len(addrs))
	for i, addr := range addrs {
		wg.Go(func() {
			errCh <- c.flushAllOnAddr(ctx, addr, delayFor(i), quiet)
		})
	}
	wg.Wait()
	close(errCh)
	var errs []error
	for err := range errCh {
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("request failed: %w", errors.Join(errs...))
	}
	return nil
}

// flushAllOnAddr sends flush_all to one node, using Quiet when requested.
func (c *Client) flushAllOnAddr(ctx context.Context, node *addr, delay Exptime, quiet bool) error {
	if node != nil && isUDPNetwork(node.Network) {
		return fmt.Errorf("udp flush_all: %w", ErrNotSupported)
	}
	req, resp := buildFlushAllCommand(delay, quiet)
	defer releaseReqAndResp(req, resp)
	if quiet {
		return c.sendQuietToAddr(ctx, node, req, resp, true)
	}

	cn, err := c.getConn(ctx, node)
	if err != nil {
		c.noteFailure(node)
		return err
	}
	var opErr error
	defer func() { finishConn(cn, opErr) }()

	c.enableUDPFraming(node, req, resp)
	stop := prepareOpIO(ctx, cn, c.rt.readTimeout, c.rt.writeTimeout)
	defer stop()
	if err := req.sendReady(ctx, cn); err != nil {
		opErr = wrapTransport(fmt.Errorf("send failed: %w", err))
		c.noteFailure(node)
		return opErr
	}
	if err := resp.recvReady(ctx, cn); err != nil {
		opErr = wrapRecvError(fmt.Errorf("recv failed: %w", err))
		c.noteFailure(node)
		return opErr
	}
	if err := mapStatusLine(resp, _OKCRLFBytes); err != nil {
		opErr = wrapRecvError(err)
		if isKeepaliveProtocolError(opErr) {
			c.noteSuccess(node)
		} else {
			c.noteFailure(node)
		}
		return opErr
	}
	if err := ensureNoTrailingBytes(cn); err != nil {
		opErr = wrapRecvError(err)
		c.noteFailure(node)
		return opErr
	}
	c.noteSuccess(node)
	return nil
}
