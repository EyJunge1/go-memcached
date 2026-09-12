package memcached

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// NodeVersion is the VERSION token from one member.
type NodeVersion struct {
	Server  Server
	Version []byte
}

// Version reads VERSION from the first live node into dst.
// dst must be non-nil. Reusing dst is the zero-allocation path.
func (c *Client) Version(ctx context.Context, dst *[]byte) error {
	if dst == nil {
		return fmt.Errorf("version dst: %w", ErrInvalidArgument)
	}
	req, resp := buildVersionCommand()
	defer releaseReqAndResp(req, resp)

	resp.versionDst = dst
	resp.afterRecv = mapVersionReply
	if err := c.dispatchFirstLive(ctx, req, resp); err != nil {
		*dst = (*dst)[:0]
		return fmt.Errorf("request: %w", err)
	}
	return nil
}

// VersionAll reads VERSION from every node into dst.
// dst must be non-nil. Partial errors are joined; successful members stay in dst.
func (c *Client) VersionAll(ctx context.Context, dst *[]NodeVersion) (err error) {
	if dst == nil {
		return fmt.Errorf("version dst: %w", ErrInvalidArgument)
	}
	start := time.Now()
	addrs, _ := c.snapshotCluster()
	defer func() { c.observeOp(ctx, start, OpVersion, nil, len(addrs), err) }()
	acc := (*dst)[:0]
	if cap(acc) < len(addrs) {
		acc = make([]NodeVersion, 0, len(addrs))
	}
	var mu sync.Mutex
	err = c.broadcastRequest(ctx, func(ctx context.Context, node *addr, h *connHolder) error {
		req, resp := buildVersionCommand()
		defer releaseReqAndResp(req, resp)
		if err := c.sendOnConn(ctx, node, h, req, resp); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		i := len(acc)
		if i < cap(acc) {
			acc = acc[:i+1]
		} else {
			acc = append(acc, NodeVersion{})
		}
		acc[i].Server = addrToServer(node)
		if perr := parseVersionInto(resp.rawLines, &acc[i].Version); perr != nil {
			acc = acc[:i]
			return wrapRecvError(perr)
		}
		return nil
	})
	*dst = acc
	c.orderNodeVersions(*dst)
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	return nil
}

// orderNodeVersions sorts VersionAll results into configured server order.
func (c *Client) orderNodeVersions(out []NodeVersion) {
	if len(out) <= 1 {
		return
	}
	order := c.serverIndex()
	sort.SliceStable(out, func(i, j int) bool {
		return order[serverKey(out[i].Server)] < order[serverKey(out[j].Server)]
	})
}
