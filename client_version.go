package memcached

import (
	"bytes"
	"context"
	"fmt"
	"sync"
)

// Ping sends version to every configured server and fails if any node is down.
func (c *Client) Ping(ctx context.Context) error {
	call := func(ctx context.Context, addr *Addr, cn memcachedConn) error {
		req, resp := buildVersionCommand()
		defer releaseReqAndResp(req, resp)

		c.enableUDPFraming(addr, req, resp)

		if err := req.send(ctx, cn, c.options.writeTimeout); err != nil {
			return wrapTransport(fmt.Errorf("send failed: %w", err))
		}
		if err := resp.recv(ctx, cn, c.options.readTimeout); err != nil {
			return wrapRecvError(fmt.Errorf("recv failed: %w", err))
		}
		if len(resp.rawLines) == 0 || !bytes.HasPrefix(resp.rawLines[0], _VersionBytes) {
			return wrapRecvError(fmt.Errorf("unexpected ping response: %w", ErrMalformedResponse))
		}
		return nil
	}

	if err := c.broadcastRequest(ctx, call); err != nil {
		return fmt.Errorf("ping failed: %w", err)
	}
	return nil
}

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

// VersionAll returns each node's version string keyed by Addr.id().
// Partial failures are joined; successful nodes remain in the map.
func (c *Client) VersionAll(ctx context.Context) (map[string]string, error) {
	out := make(map[string]string)
	var mu sync.Mutex
	err := c.broadcastRequest(ctx, func(ctx context.Context, addr *Addr, cn memcachedConn) error {
		req, resp := buildVersionCommand()
		defer releaseReqAndResp(req, resp)
		c.enableUDPFraming(addr, req, resp)
		if err := req.send(ctx, cn, c.options.writeTimeout); err != nil {
			return wrapTransport(fmt.Errorf("send failed: %w", err))
		}
		if err := resp.recv(ctx, cn, c.options.readTimeout); err != nil {
			return wrapRecvError(fmt.Errorf("recv failed: %w", err))
		}
		ver, err := parseVersionLine(resp.rawLines)
		if err != nil {
			return wrapRecvError(err)
		}
		mu.Lock()
		out[addr.id()] = ver
		mu.Unlock()
		return nil
	})
	if err != nil {
		return out, fmt.Errorf("VersionAll: %w", err)
	}
	return out, nil
}
