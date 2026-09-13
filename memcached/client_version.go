package memcached

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// NodeVersion is one node's VERSION reply.
type NodeVersion struct {
	Server  Server
	Version string
}

// Version returns the version string from the first live node
// (first Servers() entry not in DeadServers(); if all are dead, Servers()[0]).
func (c *Client) Version(ctx context.Context) (string, error) {
	req, resp := buildVersionCommand()
	defer releaseReqAndResp(req, resp)

	var version string
	resp.afterRecv = func(r *response) error {
		var err error
		version, err = parseVersionLine(r.rawLines)
		return err
	}
	if err := c.dispatchFirstLive(ctx, req, resp); err != nil {
		return "", fmt.Errorf("request: %w", err)
	}

	return version, nil
}

// VersionAll returns each node's version. Partial failures are joined;
// successful nodes remain in the slice.
func (c *Client) VersionAll(ctx context.Context) ([]NodeVersion, error) {
	var out []NodeVersion
	var mu sync.Mutex
	err := c.broadcastRequest(ctx, func(ctx context.Context, node *addr, cn memcachedConn) error {
		req, resp := buildVersionCommand()
		defer releaseReqAndResp(req, resp)
		if err := c.sendOnConn(ctx, node, cn, req, resp); err != nil {
			return err
		}
		ver, err := parseVersionLine(resp.rawLines)
		if err != nil {
			return wrapRecvError(err)
		}
		mu.Lock()
		out = append(out, NodeVersion{Server: addrToServer(node), Version: ver})
		mu.Unlock()
		return nil
	})
	if err != nil {
		c.orderNodeVersions(out)
		return out, fmt.Errorf("VersionAll: %w", err)
	}
	c.orderNodeVersions(out)
	return out, nil
}

func (c *Client) orderNodeVersions(out []NodeVersion) {
	order := c.serverIndex()
	sort.SliceStable(out, func(i, j int) bool {
		return order[serverKey(out[i].Server)] < order[serverKey(out[j].Server)]
	})
}
