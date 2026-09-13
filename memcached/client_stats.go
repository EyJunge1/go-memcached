package memcached

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"unicode"
)

func statsArgsRestricted(args []string) bool {
	for _, a := range args {
		switch strings.ToLower(a) {
		case "reset", "sizes":
			return true
		}
	}
	return false
}

func validFreeStatsArgs(args []string) error {
	for _, a := range args {
		if a == "" {
			return fmt.Errorf("stats arg empty: %w", ErrInvalidArgument)
		}
		if strings.EqualFold(a, "cachedump") {
			return fmt.Errorf("stats cachedump: %w", ErrInvalidArgument)
		}
		if strings.EqualFold(a, "detail") {
			return fmt.Errorf("stats detail: use Admin().On().StatsDetail: %w", ErrInvalidArgument)
		}
		for _, r := range a {
			if r <= ' ' || r == 0x7f || unicode.IsControl(r) {
				return fmt.Errorf("stats arg %q: %w", a, ErrInvalidArgument)
			}
		}
	}
	return nil
}

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

// StatsAll returns stats for every node keyed by network + address.
func (c *Client) StatsAll(ctx context.Context, args ...string) (map[string]map[string]string, error) {
	if err := validFreeStatsArgs(args); err != nil {
		return nil, err
	}
	if statsArgsRestricted(args) {
		return nil, fmt.Errorf("stats %q requires Admin: %w", strings.Join(args, " "), ErrAdminNotEnabled)
	}
	out := make(map[string]map[string]string)
	var mu sync.Mutex
	err := c.broadcastRequest(ctx, func(ctx context.Context, node *addr, cn memcachedConn) error {
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
		out[node.id()] = m
		mu.Unlock()
		return nil
	})
	if err != nil {
		return out, fmt.Errorf("StatsAll: %w", err)
	}
	return out, nil
}
