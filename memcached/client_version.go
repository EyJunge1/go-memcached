package memcached

import (
	"context"
	"fmt"
	"sync"
)

// Version returns the server version string.
func (c *Client) Version(ctx context.Context) (string, error) {
	req, resp := buildVersionCommand()
	defer releaseReqAndResp(req, resp)

	var version string
	resp.afterRecv = func(r *response) error {
		var err error
		version, err = parseVersionLine(r.rawLines)
		return err
	}
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return "", fmt.Errorf("request: %w", err)
	}

	return version, nil
}

// VersionAll returns each node's version string keyed by network + address.
// Partial failures are joined; successful nodes remain in the map.
func (c *Client) VersionAll(ctx context.Context) (map[string]string, error) {
	out := make(map[string]string)
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
		out[node.id()] = ver
		mu.Unlock()
		return nil
	})
	if err != nil {
		return out, fmt.Errorf("VersionAll: %w", err)
	}
	return out, nil
}
