package memcached

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// FlushOpts controls flush_all delay, cluster stagger, and noreply.
// Delay 0 omits the token (immediate). ExptimeImmediate sends wire -1.
// Stagger applies only when Delay is 0: node i uses Rel(i*Stagger).
type FlushOpts struct {
	Delay   Exptime
	Stagger time.Duration
	Quiet   bool
}

func flushDelayFor(o FlushOpts) func(int) Exptime {
	return func(i int) Exptime {
		if o.Delay != 0 {
			return o.Delay
		}
		return Rel(time.Duration(i) * o.Stagger)
	}
}

func flushOptsValid(o FlushOpts) error {
	if o.Stagger < 0 {
		return fmt.Errorf("flush_all: %w", ErrInvalidArgument)
	}
	if o.Delay != 0 && o.Stagger != 0 {
		return fmt.Errorf("flush_all Delay excludes Stagger: %w", ErrInvalidArgument)
	}
	return nil
}

// FlushAll invalidates items on the On() node. Requires Config.AllowFlushAll.
// Broadcast is All().FlushAll.
func (a *AdminClient) FlushAll(ctx context.Context, o FlushOpts) error {
	if a == nil || a.c == nil {
		return ErrAdminNotEnabled
	}
	if err := flushOptsValid(o); err != nil {
		return err
	}
	if a.target == nil {
		return ErrAdminTargetRequired
	}
	addr, err := a.resolveTarget()
	if err != nil {
		return err
	}
	if !a.c.options.allowFlushAll {
		return ErrFlushAllNotAllowed
	}
	return a.c.flushAllOnAddr(ctx, addr, flushDelayFor(o)(0), o.Quiet)
}

// FlushAll invalidates items on every configured server.
func (b *AdminBroadcast) FlushAll(ctx context.Context, o FlushOpts) error {
	if b == nil || b.c == nil {
		return ErrAdminNotEnabled
	}
	if err := flushOptsValid(o); err != nil {
		return err
	}
	return b.c.flushAllBroadcast(ctx, flushDelayFor(o), o.Quiet)
}

func (c *Client) flushAllBroadcast(ctx context.Context, delayFor func(int) Exptime, quiet bool) error {
	if !c.options.allowFlushAll {
		return ErrFlushAllNotAllowed
	}

	addrs, _ := c.snapshotCluster()
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
	if err := req.send(ctx, cn, c.options.writeTimeout); err != nil {
		opErr = wrapTransport(fmt.Errorf("send failed: %w", err))
		c.noteFailure(node)
		return opErr
	}
	if err := resp.recv(ctx, cn, c.options.readTimeout); err != nil {
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
	c.noteSuccess(node)
	return nil
}
