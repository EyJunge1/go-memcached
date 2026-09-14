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
		if strings.EqualFold(a, "reset") || strings.EqualFold(a, "sizes") {
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

// NodeStats is the STAT list from one member.
type NodeStats struct {
	Server Server
	Stats  []Stat
}

func (c *Client) statsIntoOnAddr(ctx context.Context, node *addr, dst *[]Stat, args []string) error {
	sub := strings.Join(args, " ")
	req, resp := buildStatsCommand(sub)
	defer releaseReqAndResp(req, resp)
	resp.statsDst = dst
	resp.afterRecv = mapStatsReply
	if err := c.dispatchToAddr(ctx, node, req, resp); err != nil {
		*dst = (*dst)[:0]
		return fmt.Errorf("request failed: %w", err)
	}
	return nil
}

// Stats reads STAT pairs from the first live node into dst.
// dst must be non-nil. Reusing dst is the zero-allocation path when caps fit.
// reset, sizes, detail, and cachedump are refused; TCP only.
func (c *Client) Stats(ctx context.Context, dst *[]Stat, args ...string) error {
	if dst == nil {
		return fmt.Errorf("stats dst: %w", ErrInvalidArgument)
	}
	if err := validFreeStatsArgs(args); err != nil {
		return err
	}
	sub := strings.Join(args, " ")
	if statsArgsRestricted(args) {
		return fmt.Errorf("stats %q requires Admin: %w", sub, ErrAdminNotEnabled)
	}
	req, resp := buildStatsCommand(sub)
	defer releaseReqAndResp(req, resp)
	resp.statsDst = dst
	resp.afterRecv = mapStatsReply
	if err := c.dispatchFirstLive(ctx, req, resp); err != nil {
		*dst = (*dst)[:0]
		return fmt.Errorf("request failed: %w", err)
	}
	return nil
}

// StatsAll collects stats from every configured node into dst.
// dst must be non-nil. Filter and TCP-only match Stats.
func (c *Client) StatsAll(ctx context.Context, dst *[]NodeStats, args ...string) error {
	if dst == nil {
		return fmt.Errorf("stats dst: %w", ErrInvalidArgument)
	}
	if err := validFreeStatsArgs(args); err != nil {
		return err
	}
	if statsArgsRestricted(args) {
		return fmt.Errorf("stats %q requires Admin: %w", strings.Join(args, " "), ErrAdminNotEnabled)
	}
	sub := strings.Join(args, " ")
	acc := (*dst)[:0]
	var mu sync.Mutex
	err := c.broadcastRequest(ctx, func(ctx context.Context, node *addr, cn *wireConn) error {
		req, resp := buildStatsCommand(sub)
		defer releaseReqAndResp(req, resp)
		var slot []Stat
		resp.statsDst = &slot
		resp.afterRecv = mapStatsReply
		if err := c.sendOnConn(ctx, node, cn, req, resp); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		i := len(acc)
		if i < cap(acc) {
			acc = acc[:i+1]
		} else {
			acc = append(acc, NodeStats{})
		}
		acc[i].Server = addrToServer(node)
		acc[i].Stats = slot
		return nil
	})
	*dst = acc
	c.orderNodeStats(*dst)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	return nil
}

// orderNodeStats sorts StatsAll results into configured server order.
func (c *Client) orderNodeStats(out []NodeStats) {
	if len(out) <= 1 {
		return
	}
	order := c.serverIndex()
	sort.SliceStable(out, func(i, j int) bool {
		return order[serverKey(out[i].Server)] < order[serverKey(out[j].Server)]
	})
}
