package memcached

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// StatsAll returns general-purpose stats for every node keyed by Addr.id().
// Partial failures are joined; successful nodes remain in the map.
func (c *Client) StatsAll(ctx context.Context) (map[string]*Statistic, error) {
	out := make(map[string]*Statistic)
	var mu sync.Mutex
	err := c.broadcastRequest(ctx, func(ctx context.Context, addr *Addr, cn memcachedConn) error {
		req, resp := buildStatsCommand("")
		defer releaseReqAndResp(req, resp)
		c.enableUDPFraming(addr, req, resp)
		if err := req.send(ctx, cn, c.options.writeTimeout); err != nil {
			return wrapTransport(fmt.Errorf("send failed: %w", err))
		}
		if err := resp.recv(ctx, cn, c.options.readTimeout); err != nil {
			return wrapRecvError(fmt.Errorf("recv failed: %w", err))
		}
		stat, err := parseStats(resp.rawLines)
		if err != nil {
			return wrapRecvError(err)
		}
		mu.Lock()
		out[addr.id()] = stat
		mu.Unlock()
		return nil
	})
	if err != nil {
		return out, fmt.Errorf("StatsAll: %w", err)
	}
	return out, nil
}

// Stats returns server statistics.
func (c *Client) Stats(ctx context.Context) (*Statistic, error) {
	req, resp := buildStatsCommand("")
	defer releaseReqAndResp(req, resp)

	var stat *Statistic
	resp.afterRecv = func(r *response) error {
		var err error
		stat, err = parseStats(r.rawLines)
		return err
	}
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	return stat, nil
}

var allowedStatsArgs = map[string]struct{}{
	"items":    {},
	"slabs":    {},
	"settings": {},
	"sizes":    {},
	"conns":    {},
	"reset":    {},
	"detail":   {},
	"extstore": {},
	"proxy":    {},
}

func validStatsArgs(args []string) error {
	if len(args) == 0 {
		return nil
	}
	for _, a := range args {
		if _, ok := allowedStatsArgs[a]; !ok {
			return fmt.Errorf("stats arg %q: %w", a, ErrInvalidArgument)
		}
	}
	return nil
}

// StatsArgs sends stats with an allowlisted argument (items, slabs, settings,
// sizes, conns, reset, detail, extstore, proxy). sizes can block the server.
// reset is destructive. cachedump is rejected (key dump).
func (c *Client) StatsArgs(ctx context.Context, args ...string) (map[string]string, error) {
	if err := validStatsArgs(args); err != nil {
		return nil, err
	}
	sub := strings.Join(args, " ")
	req, resp := buildStatsCommand(sub)
	defer releaseReqAndResp(req, resp)

	var out map[string]string
	resp.afterRecv = func(r *response) error {
		var err error
		out, err = parseStatsMap(r.rawLines)
		return err
	}
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	return out, nil
}
