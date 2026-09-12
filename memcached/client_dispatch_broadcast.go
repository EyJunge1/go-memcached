package memcached

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

func (c *Client) sendOnConn(
	ctx context.Context,
	node *addr,
	h *connHolder,
	req *request,
	resp *response,
) error {
	if h == nil || h.cn == nil {
		return fmt.Errorf("missing connection: %w", ErrTransport)
	}
	c.enableUDPFraming(node, req, resp)
	if req.udpEnabled {
		if err := udpRequestAllowed(req); err != nil {
			return err
		}
		fresh, err := c.rotateUDPConnIfStale(ctx, node, h.cn)
		if err != nil {
			h.cn = nil
			return err
		}
		h.cn = fresh
	}
	if resp != nil {
		resp.maxValueBytes = c.valueLimit()
		resp.readTimeout = c.rt.readTimeout
		resp.readCtx = ctx
		resp.readConn = h.cn
	}
	stop := prepareOpIO(ctx, h.cn, c.rt.readTimeout, c.rt.writeTimeout)
	defer stop()
	if err := req.sendReady(ctx, h.cn); err != nil {
		return wrapTransport(fmt.Errorf("send: %w", err))
	}
	if err := resp.recvReady(ctx, h.cn); err != nil {
		return wrapRecvError(fmt.Errorf("recv: %w", err))
	}
	if resp.afterRecv != nil {
		if err := resp.afterRecv(resp); err != nil {
			return wrapRecvError(err)
		}
	}
	if err := ensureNoTrailingBytes(h.cn); err != nil {
		return wrapRecvError(err)
	}
	return nil
}

// enableUDPFraming attaches a shared request id to req and resp when node uses UDP.
func (c *Client) enableUDPFraming(node *addr, req *request, resp *response) {
	if req == nil || resp == nil {
		return
	}
	if node == nil || !node.udp {
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
	if len(addrs) == 1 {
		return c.broadcastOne(ctx, addrs[0], call)
	}

	var (
		wg sync.WaitGroup
		mu sync.Mutex
	)
	errs := make([]error, 0, len(addrs))

	for _, addr := range addrs {
		wg.Go(func() {
			cn, err := c.getConn(ctx, addr)
			if err != nil {
				c.noteFailure(addr)
				mu.Lock()
				defer mu.Unlock()
				errs = append(errs, err)
				return
			}

			h := &connHolder{cn: cn}
			var opErr error
			defer func() { finishDispatch(h.cn, opErr) }()

			opErr = call(ctx, addr, h)
			if opErr != nil {
				c.noteFailure(addr)
				mu.Lock()
				defer mu.Unlock()
				errs = append(errs, opErr)
				return
			}
			c.noteSuccess(addr)
		})
	}

	wg.Wait()
	return errors.Join(errs...)
}

func (c *Client) broadcastOne(ctx context.Context, node *addr, call callFunc) error {
	cn, err := c.getConn(ctx, node)
	if err != nil {
		c.noteFailure(node)
		return err
	}
	h := &connHolder{cn: cn}
	var opErr error
	defer func() { finishDispatch(h.cn, opErr) }()
	opErr = call(ctx, node, h)
	if opErr != nil {
		c.noteFailure(node)
		return opErr
	}
	c.noteSuccess(node)
	return nil
}

// dispatchRequest picks a live cluster member for req.key and runs the request there.
func (c *Client) dispatchRequest(ctx context.Context, req *request, resp *response) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	start := time.Now()
	node, err := c.pickNode(req.key)
	if err != nil {
		if !observeSkipped(ctx) {
			c.observeOp(ctx, start, opName(req), nil, 1, err)
		}
		return err
	}
	return c.dispatchToAddr(ctx, node, req, resp)
}

// dispatchFirstLive writes to the first live node and picks again on each
