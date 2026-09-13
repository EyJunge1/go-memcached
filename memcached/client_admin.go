package memcached

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
)

// AdminClient exposes destructive and operational server commands.
// Obtain it via Client.Admin after Config.AllowAdmin.
// Node-local commands require On(addr). Shutdown cannot run without On.
type AdminClient struct {
	c      *Client
	target *Server
}

// AdminBroadcast runs selected ops on every configured node.
type AdminBroadcast struct {
	c *Client
}

// AdminOpts selects optional tokens for verbosity and cache_memlimit.
type AdminOpts struct {
	Quiet bool
}

// Admin returns the admin API. Requires Config.AllowAdmin.
func (c *Client) Admin() (*AdminClient, error) {
	if c == nil || c.options == nil || !c.options.allowAdmin {
		return nil, ErrAdminNotEnabled
	}
	return &AdminClient{c: c}, nil
}

// On binds the admin handle to one configured server.
func (a *AdminClient) On(addr Server) *AdminClient {
	if a == nil {
		return nil
	}
	cp := *a
	s := addr
	cp.target = &s
	return &cp
}

// All returns a broadcast handle for non-destructive cluster-wide admin ops.
func (a *AdminClient) All() *AdminBroadcast {
	if a == nil {
		return nil
	}
	return &AdminBroadcast{c: a.c}
}

func (a *AdminClient) resolveTarget() (*Addr, error) {
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

func (a *AdminClient) do(ctx context.Context, req *request, resp *response) error {
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

func (a *AdminClient) doQuiet(ctx context.Context, req *request, resp *response) error {
	addr, err := a.resolveTarget()
	if err != nil {
		return err
	}
	if err := a.c.sendQuietToAddr(ctx, addr, req, resp); err != nil {
		return fmt.Errorf("admin: %w", err)
	}
	return nil
}

func (b *AdminBroadcast) doAll(ctx context.Context, build func() (*request, *response)) error {
	if b == nil || b.c == nil {
		return ErrAdminNotEnabled
	}
	return b.c.broadcastRequest(ctx, func(ctx context.Context, addr *Addr, cn memcachedConn) error {
		req, resp := build()
		defer releaseReqAndResp(req, resp)
		if err := b.c.sendOnConn(ctx, addr, cn, req, resp); err != nil {
			return err
		}
		return parseAdminReply(resp)
	})
}

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
			errCh <- b.c.sendQuietToAddr(ctx, addr, req, resp)
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

// Verbosity sets the server log level on the bound node.
func (a *AdminClient) Verbosity(ctx context.Context, level int, o AdminOpts) error {
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

// Verbosity sets the log level on every node.
func (b *AdminBroadcast) Verbosity(ctx context.Context, level int, o AdminOpts) error {
	if level < 0 {
		return fmt.Errorf("verbosity: %w", ErrInvalidArgument)
	}
	if o.Quiet {
		return b.doAllQuiet(ctx, func() (*request, *response) { return buildVerbosityCommand(level, true) })
	}
	return b.doAll(ctx, func() (*request, *response) { return buildVerbosityCommand(level, false) })
}

// Shutdown asks the bound server to stop. Requires On().
func (a *AdminClient) Shutdown(ctx context.Context, graceful bool) error {
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

// CacheMemLimit sets the runtime cache limit in megabytes.
func (a *AdminClient) CacheMemLimit(ctx context.Context, megabytes uint32, o AdminOpts) error {
	req, resp := buildCacheMemLimitCommand(megabytes, o.Quiet)
	defer releaseReqAndResp(req, resp)
	if o.Quiet {
		return a.doQuiet(ctx, req, resp)
	}
	return a.do(ctx, req, resp)
}

// CacheMemLimit sets the cache limit on every node.
func (b *AdminBroadcast) CacheMemLimit(ctx context.Context, megabytes uint32, o AdminOpts) error {
	if o.Quiet {
		return b.doAllQuiet(ctx, func() (*request, *response) { return buildCacheMemLimitCommand(megabytes, true) })
	}
	return b.doAll(ctx, func() (*request, *response) { return buildCacheMemLimitCommand(megabytes, false) })
}

// Stats returns STAT pairs from the bound node, including reset/sizes.
func (a *AdminClient) Stats(ctx context.Context, args ...string) (map[string]string, error) {
	addr, err := a.resolveTarget()
	if err != nil {
		return nil, err
	}
	if err := validFreeStatsArgs(args); err != nil {
		return nil, err
	}
	return a.c.statsMapOnAddr(ctx, addr, args)
}

// Stats returns STAT pairs from every node.
func (b *AdminBroadcast) Stats(ctx context.Context, args ...string) (map[string]map[string]string, error) {
	if b == nil || b.c == nil {
		return nil, ErrAdminNotEnabled
	}
	if err := validFreeStatsArgs(args); err != nil {
		return nil, err
	}
	out := make(map[string]map[string]string)
	var mu sync.Mutex
	err := b.c.broadcastRequest(ctx, func(ctx context.Context, addr *Addr, cn memcachedConn) error {
		req, resp := buildStatsCommand(strings.Join(args, " "))
		defer releaseReqAndResp(req, resp)
		if err := b.c.sendOnConn(ctx, addr, cn, req, resp); err != nil {
			return err
		}
		m, err := parseStatsMap(resp.rawLines)
		if err != nil {
			return wrapRecvError(err)
		}
		mu.Lock()
		out[addr.id()] = m
		mu.Unlock()
		return nil
	})
	return out, err
}

// SlabsReassign moves memory from source to dest slab class. source -1 means any.
func (a *AdminClient) SlabsReassign(ctx context.Context, source, dest int) error {
	req, resp := buildSlabsReassignCommand(source, dest)
	defer releaseReqAndResp(req, resp)
	return a.do(ctx, req, resp)
}

// SlabsAutomove sets automove mode: 0 standby, 1 normal, 2 aggressive.
func (a *AdminClient) SlabsAutomove(ctx context.Context, mode int) error {
	if mode < 0 || mode > 2 {
		return fmt.Errorf("slabs automove: %w", ErrInvalidArgument)
	}
	req, resp := buildSlabsAutomoveCommand(mode)
	defer releaseReqAndResp(req, resp)
	return a.do(ctx, req, resp)
}

// LRUTune updates segmented LRU percentages and age factors.
func (a *AdminClient) LRUTune(ctx context.Context, hotPct, warmPct int, hotFactor, warmFactor float64) error {
	if hotPct < 0 || warmPct < 0 || hotFactor < 0 || warmFactor < 0 {
		return fmt.Errorf("lru tune: %w", ErrInvalidArgument)
	}
	req, resp := buildLRUTuneCommand(hotPct, warmPct, hotFactor, warmFactor)
	defer releaseReqAndResp(req, resp)
	return a.do(ctx, req, resp)
}

// LRUMode sets flat or segmented LRU.
func (a *AdminClient) LRUMode(ctx context.Context, mode string) error {
	if mode != "flat" && mode != "segmented" {
		return fmt.Errorf("lru mode: %w", ErrInvalidArgument)
	}
	req, resp := buildLRUModeCommand(mode)
	defer releaseReqAndResp(req, resp)
	return a.do(ctx, req, resp)
}

// LRUTempTTL sets the TEMP_LRU threshold. Negative disables TEMP_LRU.
func (a *AdminClient) LRUTempTTL(ctx context.Context, ttl int) error {
	req, resp := buildLRUTempTTLCommand(ttl)
	defer releaseReqAndResp(req, resp)
	return a.do(ctx, req, resp)
}

// LRUCrawlerEnable enables or disables the LRU crawler.
func (a *AdminClient) LRUCrawlerEnable(ctx context.Context, on bool) error {
	arg := "disable"
	if on {
		arg = "enable"
	}
	req, resp := buildLRUCrawlerCommand(arg)
	defer releaseReqAndResp(req, resp)
	return a.do(ctx, req, resp)
}

// LRUCrawlerSleep sets crawler sleep in microseconds (0–1_000_000).
func (a *AdminClient) LRUCrawlerSleep(ctx context.Context, usec uint32) error {
	if usec > 1_000_000 {
		return fmt.Errorf("lru_crawler sleep: %w", ErrInvalidArgument)
	}
	req, resp := buildLRUCrawlerCommand("sleep", strconvU32(usec))
	defer releaseReqAndResp(req, resp)
	return a.do(ctx, req, resp)
}

// LRUCrawlerToCrawl sets max items per class per crawler run.
func (a *AdminClient) LRUCrawlerToCrawl(ctx context.Context, n uint32) error {
	req, resp := buildLRUCrawlerCommand("tocrawl", strconvU32(n))
	defer releaseReqAndResp(req, resp)
	return a.do(ctx, req, resp)
}

// LRUCrawlerCrawl starts a crawl of the given classes ("all" or "1,3,10").
func (a *AdminClient) LRUCrawlerCrawl(ctx context.Context, classes string) error {
	if err := validCrawlerSelector(classes); err != nil {
		return err
	}
	req, resp := buildLRUCrawlerCommand("crawl", classes)
	defer releaseReqAndResp(req, resp)
	return a.do(ctx, req, resp)
}

func strconvU32(n uint32) string {
	return fmt.Sprintf("%d", n)
}

func requireDeadline(ctx context.Context) error {
	if ctx == nil {
		return ErrDeadlineRequired
	}
	if _, ok := ctx.Deadline(); !ok {
		return ErrDeadlineRequired
	}
	return nil
}
