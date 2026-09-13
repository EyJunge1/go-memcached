package memcached

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// FlushOpts controls flush_all delay, cluster stagger, and noreply.
// Exp, when non-zero, is sent as the delay token (including ExptimeImmediate).
// Delay is used only when Exp is 0 and maps through Rel; 0 omits the token (immediate).
// Delay < 0 is invalid (not ExptimeImmediate); immediate flush is Delay 0 or Exp: ExptimeImmediate.
// Exp cannot be combined with Delay or Stagger.
type FlushOpts struct {
	Delay   time.Duration // used when Exp == 0; 0 omits the delay token; negative is ErrInvalidArgument
	Exp     Exptime       // when != 0, sent as the flush_all delay token
	Stagger time.Duration
	Quiet   bool
}

func flushDelayFor(o FlushOpts) func(int) Exptime {
	return func(i int) Exptime {
		if o.Exp != 0 {
			return o.Exp
		}
		return Rel(o.Delay + time.Duration(i)*o.Stagger)
	}
}

func flushOptsValid(o FlushOpts) error {
	if o.Delay < 0 || o.Stagger < 0 {
		return fmt.Errorf("flush_all: %w", ErrInvalidArgument)
	}
	if o.Exp != 0 && (o.Delay != 0 || o.Stagger != 0) {
		return fmt.Errorf("flush_all Exp excludes Delay/Stagger: %w", ErrInvalidArgument)
	}
	return nil
}

// FlushAll invalidates items on every configured server when the handle is
// unbound. On(addr).FlushAll hits only that node. Requires Config.AllowFlushAll.
func (a *AdminClient) FlushAll(ctx context.Context, o FlushOpts) error {
	if a == nil || a.c == nil {
		return ErrAdminNotEnabled
	}
	if err := flushOptsValid(o); err != nil {
		return err
	}
	if a.target != nil {
		addr, err := a.resolveTarget()
		if err != nil {
			return err
		}
		if !a.c.options.allowFlushAll {
			return ErrFlushAllNotAllowed
		}
		return a.c.flushAllOnAddr(ctx, addr, flushDelayFor(o)(0), o.Quiet)
	}
	return a.c.flushAllBroadcast(ctx, flushDelayFor(o), o.Quiet)
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

func (c *Client) flushAllOnAddr(ctx context.Context, addr *Addr, delay Exptime, quiet bool) error {
	if addr != nil && isUDPNetwork(addr.Network) {
		return fmt.Errorf("udp flush_all: %w", ErrNotSupported)
	}
	req, resp := buildFlushAllCommand(delay, quiet)
	defer releaseReqAndResp(req, resp)
	if quiet {
		return c.sendQuietToAddr(ctx, addr, req, resp)
	}

	cn, err := c.getConn(ctx, addr)
	if err != nil {
		c.noteFailure(addr)
		return err
	}
	var opErr error
	defer func() { finishConn(cn, opErr) }()

	c.enableUDPFraming(addr, req, resp)
	if err := req.send(ctx, cn, c.options.writeTimeout); err != nil {
		opErr = wrapTransport(fmt.Errorf("send failed: %w", err))
		c.noteFailure(addr)
		return opErr
	}
	if err := resp.recv(ctx, cn, c.options.readTimeout); err != nil {
		opErr = wrapRecvError(fmt.Errorf("recv failed: %w", err))
		c.noteFailure(addr)
		return opErr
	}
	if err := mapStatusLine(resp, _OKCRLFBytes); err != nil {
		opErr = wrapRecvError(err)
		if isKeepaliveProtocolError(opErr) {
			c.noteSuccess(addr)
		} else {
			c.noteFailure(addr)
		}
		return opErr
	}
	c.noteSuccess(addr)
	return nil
}
