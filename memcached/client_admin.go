package memcached

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
)

// Admin is the factory for On and All handles.
// Get it from Client.Admin when Config.AllowAdmin is set.
type Admin struct {
	c *Client
}

// AdminNode runs admin commands on one configured member.
type AdminNode struct {
	c      *Client
	target *Server
}

// AdminBroadcast runs the subset of admin ops that may hit every node.
type AdminBroadcast struct {
	c *Client
}

// AdminOpts adds optional tokens for verbosity and cache_memlimit.
type AdminOpts struct {
	Quiet bool
}

// StatsDetailMode is the on/off/dump argument to AdminNode.StatsDetail.
type StatsDetailMode uint8

const (
	StatsDetailOn StatsDetailMode = iota
	StatsDetailOff
	StatsDetailDump
)

// wire maps the mode onto the on/off/dump token stats detail expects.
func (m StatsDetailMode) wire() (string, error) {
	switch m {
	case StatsDetailOn:
		return "on", nil
	case StatsDetailOff:
		return "off", nil
	case StatsDetailDump:
		return "dump", nil
	default:
		return "", fmt.Errorf("stats detail: %w", ErrInvalidArgument)
	}
}

// Admin returns the admin factory. Config.AllowAdmin must be set.
func (c *Client) Admin() (*Admin, error) {
	if c == nil || c.rt == nil || !c.rt.allowAdmin {
		return nil, ErrAdminNotEnabled
	}
	c.mu.RLock()
	closed := c.closed
	c.mu.RUnlock()
	if closed {
		return nil, ErrClosed
	}
	return &Admin{c: c}, nil
}

// On pins admin commands to one configured server.
func (a *Admin) On(server Server) *AdminNode {
	if a == nil {
		return nil
	}
	s := server
	return &AdminNode{c: a.c, target: &s}
}

// All returns a handle for cluster-wide admin ops (Flush needs AllowFlushAll).
func (a *Admin) All() *AdminBroadcast {
	if a == nil {
		return nil
	}
	return &AdminBroadcast{c: a.c}
}

// resolveTarget finds the configured member that matches the pinned AdminNode server.
func (a *AdminNode) resolveTarget() (*addr, error) {
	if a == nil || a.c == nil {
		return nil, ErrAdminNotEnabled
	}
	if a.target == nil {
		return nil, ErrAdminTargetRequired
	}
	addrs, _ := a.c.snapshotCluster()
	wantNet := serverNetwork(*a.target)
	for _, addr := range addrs {
		if addr != nil && addr.Address == a.target.Address && serverNetwork(addrToServer(addr)) == wantNet {
			return addr, nil
		}
	}
	return nil, fmt.Errorf("admin target %s: %w", a.target.Address, ErrInvalidAddress)
}

// do dispatches a request/response admin command to the pinned node and maps the reply.
func (a *AdminNode) do(ctx context.Context, req *request, resp *response) error {
	addr, err := a.resolveTarget()
	if err != nil {
		return err
	}
	resp.afterRecv = parseAdminReply
	if err := a.c.dispatchToAddr(ctx, addr, req, resp); err != nil {
		return fmt.Errorf("admin: %w", err)
	}
	return nil
}

// doQuiet sends a noreply admin command to the pinned node.
func (a *AdminNode) doQuiet(ctx context.Context, req *request, resp *response) error {
	addr, err := a.resolveTarget()
	if err != nil {
		return err
	}
	if err := a.c.sendQuietToAddr(ctx, addr, req, resp, false); err != nil {
		return fmt.Errorf("admin: %w", err)
	}
	return nil
}

// doAll runs a request/response admin command on every configured node.
func (b *AdminBroadcast) doAll(ctx context.Context, build func() (*request, *response)) error {
	if b == nil || b.c == nil {
		return ErrAdminNotEnabled
	}
	return b.c.broadcastRequest(ctx, func(ctx context.Context, node *addr, cn wireConn) error {
		req, resp := build()
		defer releaseReqAndResp(req, resp)
		if err := b.c.sendOnConn(ctx, node, cn, req, resp); err != nil {
			return err
		}
		return parseAdminReply(resp)
	})
}

// doAllQuiet sends a noreply admin command to every configured node.
func (b *AdminBroadcast) doAllQuiet(ctx context.Context, build func() (*request, *response)) error {
	if b == nil || b.c == nil {
		return ErrAdminNotEnabled
	}
	addrs, _ := b.c.snapshotCluster()
	var wg sync.WaitGroup
	errCh := make(chan error, len(addrs))
	for _, addr := range addrs {
		wg.Go(func() {
			req, resp := build()
			defer releaseReqAndResp(req, resp)
			errCh <- b.c.sendQuietToAddr(ctx, addr, req, resp, false)
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
		return fmt.Errorf("admin: %w", errors.Join(errs...))
	}
	return nil
}

// Verbosity sets the log level on the bound node.
func (a *AdminNode) Verbosity(ctx context.Context, level int, o AdminOpts) error {
	if level < 0 {
		return fmt.Errorf("verbosity: %w", ErrInvalidArgument)
	}
	req, resp := buildVerbosityCommand(level, o.Quiet)
	defer releaseReqAndResp(req, resp)
	if o.Quiet {
		return a.doQuiet(ctx, req, resp)
	}
	return a.do(ctx, req, resp)
}

// Verbosity sets the log level on every configured node.
func (b *AdminBroadcast) Verbosity(ctx context.Context, level int, o AdminOpts) error {
	if level < 0 {
		return fmt.Errorf("verbosity: %w", ErrInvalidArgument)
	}
	if o.Quiet {
		return b.doAllQuiet(ctx, func() (*request, *response) { return buildVerbosityCommand(level, true) })
	}
	return b.doAll(ctx, func() (*request, *response) { return buildVerbosityCommand(level, false) })
}

// Shutdown tells the bound server to stop. On() is required.
func (a *AdminNode) Shutdown(ctx context.Context, graceful bool) error {
	addr, err := a.resolveTarget()
	if err != nil {
		return err
	}
	req, resp := buildShutdownCommand(graceful)
	defer releaseReqAndResp(req, resp)
	resp.afterRecv = func(r *response) error {
		if len(r.rawLines) == 0 {
			return io.EOF
		}
		return parseAdminReply(r)
	}
	err = a.c.dispatchToAddr(ctx, addr, req, resp)
	if err == nil || isShutdownClosed(err) {
		return nil
	}
	return fmt.Errorf("shutdown: %w", err)
}

// CacheMemLimit sets the runtime cache size in megabytes.
func (a *AdminNode) CacheMemLimit(ctx context.Context, megabytes uint32, o AdminOpts) error {
	req, resp := buildCacheMemLimitCommand(megabytes, o.Quiet)
	defer releaseReqAndResp(req, resp)
	if o.Quiet {
		return a.doQuiet(ctx, req, resp)
	}
	return a.do(ctx, req, resp)
}

// CacheMemLimit sets the cache size on every configured node.
func (b *AdminBroadcast) CacheMemLimit(ctx context.Context, megabytes uint32, o AdminOpts) error {
	if o.Quiet {
		return b.doAllQuiet(ctx, func() (*request, *response) { return buildCacheMemLimitCommand(megabytes, true) })
	}
	return b.doAll(ctx, func() (*request, *response) { return buildCacheMemLimitCommand(megabytes, false) })
}

// StatsDetail runs stats detail on, off, or dump; dump returns PREFIX counters, on/off an empty map.
func (a *AdminNode) StatsDetail(ctx context.Context, mode StatsDetailMode) (map[string]string, error) {
	wire, err := mode.wire()
	if err != nil {
		return nil, err
	}
	addr, err := a.resolveTarget()
	if err != nil {
		return nil, err
	}
	req, resp := buildStatsDetailCommand(wire)
	defer releaseReqAndResp(req, resp)
	var out map[string]string
	if mode == StatsDetailDump {
		resp.afterRecv = func(r *response) error {
			var perr error
			out, perr = parseStatsDetailDump(r.rawLines)
			return perr
		}
	} else {
		resp.afterRecv = parseAdminReply
		out = map[string]string{}
	}
	if err := a.c.dispatchToAddr(ctx, addr, req, resp); err != nil {
		return nil, err
	}
	if out == nil {
		out = map[string]string{}
	}
	return out, nil
}

// Stats runs stats reset or stats sizes on the bound node.
// All other subcommands belong on Client.Stats or Client.StatsAll.
func (a *AdminNode) Stats(ctx context.Context, args ...string) (map[string]string, error) {
	addr, err := a.resolveTarget()
	if err != nil {
		return nil, err
	}
	if err := validAdminStatsArgs(args); err != nil {
		return nil, err
	}
	return a.c.statsMapOnAddr(ctx, addr, args)
}

// validAdminStatsArgs allows only stats reset and stats sizes on AdminNode.
func validAdminStatsArgs(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("admin stats: only reset and sizes: %w", ErrInvalidArgument)
	}
	for _, a := range args {
		if !strings.EqualFold(a, "reset") && !strings.EqualFold(a, "sizes") {
			return fmt.Errorf("admin stats %q: use Client.Stats: %w", a, ErrInvalidArgument)
		}
	}
	return validFreeStatsArgs(args)
}

// SlabsReassign moves pages from source to dest. source -1 means any class.
func (a *AdminNode) SlabsReassign(ctx context.Context, source, dest int) error {
	req, resp := buildSlabsReassignCommand(source, dest)
	defer releaseReqAndResp(req, resp)
	return a.do(ctx, req, resp)
}

// SlabsAutomove sets automove: 0 standby, 1 normal, 2 aggressive.
func (a *AdminNode) SlabsAutomove(ctx context.Context, mode int) error {
	if mode < 0 || mode > 2 {
		return fmt.Errorf("slabs automove: %w", ErrInvalidArgument)
	}
	req, resp := buildSlabsAutomoveCommand(mode)
	defer releaseReqAndResp(req, resp)
	return a.do(ctx, req, resp)
}

// LRUTune updates segmented LRU percentages and age multipliers.
func (a *AdminNode) LRUTune(ctx context.Context, hotPct, warmPct int, hotFactor, warmFactor float64) error {
	if hotPct < 0 || warmPct < 0 || hotFactor < 0 || warmFactor < 0 {
		return fmt.Errorf("lru tune: %w", ErrInvalidArgument)
	}
	req, resp := buildLRUTuneCommand(hotPct, warmPct, hotFactor, warmFactor)
	defer releaseReqAndResp(req, resp)
	return a.do(ctx, req, resp)
}

// LRUMode switches between flat and segmented LRU.
func (a *AdminNode) LRUMode(ctx context.Context, mode string) error {
	if mode != "flat" && mode != "segmented" {
		return fmt.Errorf("lru mode: %w", ErrInvalidArgument)
	}
	req, resp := buildLRUModeCommand(mode)
	defer releaseReqAndResp(req, resp)
	return a.do(ctx, req, resp)
}

// LRUTempTTL sets the TEMP_LRU window. A negative value turns TEMP_LRU off.
func (a *AdminNode) LRUTempTTL(ctx context.Context, ttl int) error {
	req, resp := buildLRUTempTTLCommand(ttl)
	defer releaseReqAndResp(req, resp)
	return a.do(ctx, req, resp)
}

// LRUCrawlerEnable turns the LRU crawler on or off.
func (a *AdminNode) LRUCrawlerEnable(ctx context.Context, on bool) error {
	arg := "disable"
	if on {
		arg = "enable"
	}
	req, resp := buildLRUCrawlerCommand(arg)
	defer releaseReqAndResp(req, resp)
	return a.do(ctx, req, resp)
}

// LRUCrawlerSleep sets crawler pause in microseconds (0 through 1_000_000).
func (a *AdminNode) LRUCrawlerSleep(ctx context.Context, usec uint32) error {
	if usec > 1_000_000 {
		return fmt.Errorf("lru_crawler sleep: %w", ErrInvalidArgument)
	}
	req, resp := buildLRUCrawlerCommand("sleep", strconvU32(usec))
	defer releaseReqAndResp(req, resp)
	return a.do(ctx, req, resp)
}

// LRUCrawlerToCrawl caps items per slab class on each crawler pass.
func (a *AdminNode) LRUCrawlerToCrawl(ctx context.Context, n uint32) error {
	req, resp := buildLRUCrawlerCommand("tocrawl", strconvU32(n))
	defer releaseReqAndResp(req, resp)
	return a.do(ctx, req, resp)
}

// LRUCrawlerCrawl starts a crawl of classes ("all" or "1,3,10").
func (a *AdminNode) LRUCrawlerCrawl(ctx context.Context, classes string) error {
	if err := validCrawlerSelector(classes); err != nil {
		return err
	}
	req, resp := buildLRUCrawlerCommand("crawl", classes)
	defer releaseReqAndResp(req, resp)
	return a.do(ctx, req, resp)
}

// strconvU32 formats n as a decimal token for admin command arguments.
func strconvU32(n uint32) string {
	return fmt.Sprintf("%d", n)
}

// requireDeadline returns ErrDeadlineRequired when ctx is nil or has no deadline.
func requireDeadline(ctx context.Context) error {
	if ctx == nil {
		return ErrDeadlineRequired
	}
	if _, ok := ctx.Deadline(); !ok {
		return ErrDeadlineRequired
	}
	return nil
}
