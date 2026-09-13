package memcached

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// FlushOpts controls flush_all delay, cluster stagger, and noreply.
type FlushOpts struct {
	Delay   time.Duration // 0 omits the delay token (immediate)
	Stagger time.Duration
	Quiet   bool
}

func flushDelayFor(o FlushOpts) func(int) Exptime {
	return func(i int) Exptime {
		return Rel(o.Delay + time.Duration(i)*o.Stagger)
	}
}

// FlushAll invalidates items on every configured server.
// It requires Admin() and Config.AllowFlushAll.
func (a *AdminClient) FlushAll(ctx context.Context, o FlushOpts) error {
	if a == nil || a.c == nil {
		return ErrAdminNotEnabled
	}
	if o.Delay < 0 || o.Stagger < 0 {
		return fmt.Errorf("flush_all: %w", ErrInvalidArgument)
	}
	return a.c.flushAllBroadcast(ctx, flushDelayFor(o), o.Quiet)
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
