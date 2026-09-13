package memcached

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"unicode"
)

// statsArgsRestricted reports whether args require Admin (reset or sizes).
func statsArgsRestricted(args []string) bool {
	for _, a := range args {
		switch strings.ToLower(a) {
		case "reset", "sizes":
			return true
		}
	}
	return false
}

// validFreeStatsArgs rejects empty, control, cachedump, and detail stats args.
func validFreeStatsArgs(args []string) error {
	for _, a := range args {
		if a == "" {
			return fmt.Errorf("stats arg empty: %w", ErrInvalidArgument)
		}
		if strings.EqualFold(a, "cachedump") {
			return fmt.Errorf("stats cachedump: %w", ErrInvalidArgument)
		}
		if strings.EqualFold(a, "detail") {
			return fmt.Errorf("stats detail: use Admin().On(server).StatsDetail: %w", ErrInvalidArgument)
		}
		for _, r := range a {
			if r <= ' ' || r == 0x7f || unicode.IsControl(r) {
				return fmt.Errorf("stats arg %q: %w", a, ErrInvalidArgument)
			}
		}
	}
	return nil
}

// NodeStats is the STAT map from one member.
type NodeStats struct {
	Server Server
	Stats  map[string]string
}

// statsMapOnAddr reads STAT pairs from one node.
func (c *Client) statsMapOnAddr(ctx context.Context, node *addr, args []string) (map[string]string, error) {
	sub := strings.Join(args, " ")
	req, resp := buildStatsCommand(sub)
	defer releaseReqAndResp(req, resp)
	var out map[string]string
	resp.afterRecv = func(r *response) error {
		var err error
		out, err = parseStatsMap(r.rawLines)
		return err
	}
	if err := c.dispatchToAddr(ctx, node, req, resp); err != nil {
		return nil, err
	}
	return out, nil
}

// Stats reads STAT pairs from the first live node. reset, sizes, detail, and cachedump are refused; TCP only.
func (c *Client) Stats(ctx context.Context, args ...string) (map[string]string, error) {
	if err := validFreeStatsArgs(args); err != nil {
		return nil, err
	}
	if statsArgsRestricted(args) {
		return nil, fmt.Errorf("stats %q requires Admin: %w", strings.Join(args, " "), ErrAdminNotEnabled)
	}
	req, resp := buildStatsCommand(strings.Join(args, " "))
	defer releaseReqAndResp(req, resp)
	var out map[string]string
	resp.afterRecv = func(r *response) error {
		var err error
		out, err = parseStatsMap(r.rawLines)
		return err
	}
	if err := c.dispatchFirstLive(ctx, req, resp); err != nil {
		return nil, err
	}
	return out, nil
}

// StatsAll collects stats from every configured node. Filter and TCP-only match Stats.
func (c *Client) StatsAll(ctx context.Context, args ...string) ([]NodeStats, error) {
	if err := validFreeStatsArgs(args); err != nil {
		return nil, err
	}
	if statsArgsRestricted(args) {
		return nil, fmt.Errorf("stats %q requires Admin: %w", strings.Join(args, " "), ErrAdminNotEnabled)
	}
	var out []NodeStats
	var mu sync.Mutex
	err := c.broadcastRequest(ctx, func(ctx context.Context, node *addr, cn wireConn) error {
		req, resp := buildStatsCommand(strings.Join(args, " "))
		defer releaseReqAndResp(req, resp)
		if err := c.sendOnConn(ctx, node, cn, req, resp); err != nil {
			return err
		}
		m, err := parseStatsMap(resp.rawLines)
		if err != nil {
			return wrapRecvError(err)
		}
		mu.Lock()
		out = append(out, NodeStats{Server: addrToServer(node), Stats: m})
		mu.Unlock()
		return nil
	})
	if err != nil {
		c.orderNodeStats(out)
		return out, fmt.Errorf("StatsAll: %w", err)
	}
	c.orderNodeStats(out)
	return out, nil
}

// orderNodeStats sorts StatsAll results into configured server order.
func (c *Client) orderNodeStats(out []NodeStats) {
	order := c.serverIndex()
	sort.SliceStable(out, func(i, j int) bool {
		return order[serverKey(out[i].Server)] < order[serverKey(out[j].Server)]
	})
}
