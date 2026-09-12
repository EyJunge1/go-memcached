package memcached

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

// StatsDetail runs stats detail on, off, or dump into dst.
// dump fills PREFIX/STAT pairs; on/off leaves dst empty. dst must be non-nil.
func (a *AdminNode) StatsDetail(ctx context.Context, dst *[]Stat, mode StatsDetailMode) error {
	if dst == nil {
		return fmt.Errorf("stats dst: %w", ErrInvalidArgument)
	}
	wire, err := mode.wire()
	if err != nil {
		return err
	}
	addr, c, err := a.resolveTarget()
	if err != nil {
		return err
	}
	req, resp := buildStatsDetailCommand(wire)
	defer releaseReqAndResp(req, resp)
	if mode == StatsDetailDump {
		resp.statsDst = dst
		resp.statsDetail = true
		resp.afterRecv = mapStatsDetailDump
	} else {
		resp.afterRecv = parseAdminReply
		*dst = (*dst)[:0]
	}
	if err := c.dispatchToAddr(ctx, addr, req, resp); err != nil {
		*dst = (*dst)[:0]
		return fmt.Errorf("request: %w", err)
	}
	return nil
}

// StatsReset runs stats reset on the bound node.
func (a *AdminNode) StatsReset(ctx context.Context) error {
	addr, c, err := a.resolveTarget()
	if err != nil {
		return err
	}
	var discard []Stat
	return c.statsIntoOnAddr(ctx, addr, &discard, []string{"reset"})
}

// StatsSizes runs stats sizes on the bound node into dst.
// dst must be non-nil. Reusing dst is the zero-allocation path when caps fit.
func (a *AdminNode) StatsSizes(ctx context.Context, dst *[]Stat) error {
	if dst == nil {
		return fmt.Errorf("stats dst: %w", ErrInvalidArgument)
	}
	addr, c, err := a.resolveTarget()
	if err != nil {
		return err
	}
	return c.statsIntoOnAddr(ctx, addr, dst, []string{"sizes"})
}

// SlabsReassign moves pages from source to dest. source -1 means any class.
func (a *AdminNode) SlabsReassign(ctx context.Context, source, dest int) error {
	req, resp, err := buildSlabsReassignCommand(source, dest)
	if err != nil {
		return err
	}
	defer releaseReqAndResp(req, resp)
	return a.do(ctx, req, resp)
}

// SlabsAutomove sets the automove policy. Zero is standby.
func (a *AdminNode) SlabsAutomove(ctx context.Context, mode SlabsAutomoveMode) error {
	wire, err := mode.wire()
	if err != nil {
		return err
	}
	req, resp, err := buildSlabsAutomoveCommand(wire)
	if err != nil {
		return err
	}
	defer releaseReqAndResp(req, resp)
	return a.do(ctx, req, resp)
}

// LRUTune updates segmented LRU percentages and age multipliers.
func (a *AdminNode) LRUTune(
	ctx context.Context,
	hotPct, warmPct int,
	hotFactor, warmFactor float64,
) error {
	if hotPct < 0 || warmPct < 0 || hotFactor < 0 || warmFactor < 0 {
		return fmt.Errorf("lru tune: %w", ErrInvalidArgument)
	}
	req, resp, err := buildLRUTuneCommand(hotPct, warmPct, hotFactor, warmFactor)
	if err != nil {
		return err
	}
	defer releaseReqAndResp(req, resp)
	return a.do(ctx, req, resp)
}

// LRUMode switches between flat and segmented LRU.
func (a *AdminNode) LRUMode(ctx context.Context, mode LRUMode) error {
	if mode != LRUFlat && mode != LRUSegmented {
		return fmt.Errorf("lru mode: %w", ErrInvalidArgument)
	}
	req, resp, err := buildLRUModeCommand(string(mode))
	if err != nil {
		return err
	}
	defer releaseReqAndResp(req, resp)
	return a.do(ctx, req, resp)
}

// LRUTempTTL sets the TEMP_LRU window. A negative value turns TEMP_LRU off.
// The duration is truncated to whole seconds on the wire.
func (a *AdminNode) LRUTempTTL(ctx context.Context, ttl time.Duration) error {
	secs := int(ttl / time.Second)
	if ttl < 0 {
		secs = -1
	}
	req, resp, err := buildLRUTempTTLCommand(secs)
	if err != nil {
		return err
	}
	defer releaseReqAndResp(req, resp)
	return a.do(ctx, req, resp)
}

// LRUCrawlerMode turns the LRU crawler on or off.
type LRUCrawlerMode int

const (
	// LRUCrawlerDisabled stops the crawler.
	LRUCrawlerDisabled LRUCrawlerMode = iota + 1
	// LRUCrawlerEnabled starts the crawler.
	LRUCrawlerEnabled
)

// LRUCrawlerEnable turns the LRU crawler on or off.
func (a *AdminNode) LRUCrawlerEnable(ctx context.Context, mode LRUCrawlerMode) error {
	arg := "disable"
	if mode == LRUCrawlerEnabled {
		arg = "enable"
	}
	req, resp := buildLRUCrawlerCommand(arg)
	defer releaseReqAndResp(req, resp)
	return a.do(ctx, req, resp)
}

// LRUCrawlerSleep sets crawler pause (0 through 1s; truncated to microseconds).
func (a *AdminNode) LRUCrawlerSleep(ctx context.Context, d time.Duration) error {
	if d < 0 || d > time.Second {
		return fmt.Errorf("lru_crawler sleep: %w", ErrInvalidArgument)
	}
	usec := uint32(d / time.Microsecond)
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
	if err := validCrawlClasses(classes); err != nil {
		return err
	}
	req, resp := buildLRUCrawlerCommand("crawl", classes)
	defer releaseReqAndResp(req, resp)
	return a.do(ctx, req, resp)
}

// strconvU32 formats n as a decimal token for admin command arguments.
func strconvU32(n uint32) string {
	return strconv.FormatUint(uint64(n), 10)
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
