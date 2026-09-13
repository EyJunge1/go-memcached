package memcached

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// FlushAll removes all items on every configured server immediately.
// It requires WithAllowFlushAll(); otherwise it returns ErrFlushAllNotAllowed
// and does not contact any node.
func (c *Client) FlushAll(ctx context.Context) error {
	return c.FlushAllAfter(ctx, 0)
}

// FlushAllAfter invalidates items after delay seconds (delay 0 is immediate).
// It requires WithAllowFlushAll().
func (c *Client) FlushAllAfter(ctx context.Context, delay time.Duration) error {
	return c.flushAllBroadcast(ctx, func(int) time.Duration { return delay })
}

// FlushAllStaggered sends flush_all with delay i*step on cluster node i
// so pool members do not expire together. Requires WithAllowFlushAll().
func (c *Client) FlushAllStaggered(ctx context.Context, step time.Duration) error {
	return c.flushAllBroadcast(ctx, func(i int) time.Duration { return time.Duration(i) * step })
}

func (c *Client) flushAllBroadcast(ctx context.Context, delayFor func(int) time.Duration) error {
	if !c.options.allowFlushAll {
		return ErrFlushAllNotAllowed
	}

	addrs, _ := c.snapshotCluster()
	var wg sync.WaitGroup
	errCh := make(chan error, len(addrs))
	for i, addr := range addrs {
		wg.Go(func() {
			errCh <- c.flushAllOnAddr(ctx, addr, delayFor(i))
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

func (c *Client) flushAllOnAddr(ctx context.Context, addr *Addr, delay time.Duration) error {
	cn, err := c.getConn(ctx, addr)
	if err != nil {
		return err
	}
	var opErr error
	defer func() { finishConn(cn, opErr) }()

	req, resp := buildFlushAllCommand(delay, false)
	defer releaseReqAndResp(req, resp)
	c.enableUDPFraming(addr, req, resp)
	if err := req.send(ctx, cn, c.options.writeTimeout); err != nil {
		opErr = wrapTransport(fmt.Errorf("send failed: %w", err))
		return opErr
	}
	if err := resp.recv(ctx, cn, c.options.readTimeout); err != nil {
		opErr = wrapRecvError(fmt.Errorf("recv failed: %w", err))
		return opErr
	}
	if err := mapStatusLine(resp, _OKCRLFBytes); err != nil {
		opErr = wrapRecvError(err)
		return opErr
	}
	return nil
}
