package memcached

import (
	"context"
	"fmt"
	"io"
)

// AdminClient exposes destructive and operational server commands.
// Obtain it via Client.Admin after WithAdminCommands().
// Commands target one picked node, not the whole cluster, except where noted.
type AdminClient struct {
	c *Client
}

// Admin returns the admin API. Requires WithAdminCommands().
func (c *Client) Admin() (*AdminClient, error) {
	if c == nil || c.options == nil || !c.options.allowAdmin {
		return nil, ErrAdminNotEnabled
	}
	return &AdminClient{c: c}, nil
}

func (a *AdminClient) do(ctx context.Context, req *request, resp *response) error {
	resp.afterRecv = parseAdminReply
	if err := a.c.dispatchRequest(ctx, req, resp); err != nil {
		return fmt.Errorf("admin: %w", err)
	}
	return nil
}

// Verbosity sets the server log level.
func (a *AdminClient) Verbosity(ctx context.Context, level int) error {
	if level < 0 {
		return fmt.Errorf("verbosity: %w", ErrInvalidArgument)
	}
	req, resp := buildVerbosityCommand(level)
	defer releaseReqAndResp(req, resp)
	return a.do(ctx, req, resp)
}

// Shutdown asks the server to stop. graceful uses "shutdown graceful".
// A closed connection after the command is treated as success.
func (a *AdminClient) Shutdown(ctx context.Context, graceful bool) error {
	req, resp := buildShutdownCommand(graceful)
	defer releaseReqAndResp(req, resp)
	resp.afterRecv = func(r *response) error {
		if len(r.rawLines) == 0 {
			return io.EOF
		}
		return parseAdminReply(r)
	}
	err := a.c.dispatchRequest(ctx, req, resp)
	if err == nil || isShutdownClosed(err) {
		return nil
	}
	return fmt.Errorf("shutdown: %w", err)
}

// CacheMemLimit sets the runtime cache limit in megabytes.
func (a *AdminClient) CacheMemLimit(ctx context.Context, megabytes uint32) error {
	req, resp := buildCacheMemLimitCommand(megabytes)
	defer releaseReqAndResp(req, resp)
	return a.do(ctx, req, resp)
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
