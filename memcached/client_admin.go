package memcached

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type (
	// Admin is the factory for On and All handles.
	// Get it from Client.Admin when Config.AllowAdmin is set.
	Admin struct {
		c *Client
	}

	// AdminNode runs admin commands on one configured member.
	AdminNode struct {
		c      *Client
		target Server
	}

	// AdminBroadcast runs the subset of admin ops that may hit every node.
	AdminBroadcast struct {
		c *Client
	}

	// AdminOpts adds optional tokens for verbosity and cache_memlimit.
	// Quiet means only that the noreply write was sent; no fault probe is made.
	// The connection is discarded because reply position cannot be proven.
	// Extra appends raw wire tokens before noreply.
	AdminOpts struct {
		Quiet bool
		Extra []string
	}

	// StatsDetailMode is the on/off/dump argument to AdminNode.StatsDetail.
	// The zero value is invalid; it does not enable detail.
	StatsDetailMode uint8
)

// StatsDetailOn enables per-key stats detail. StatsDetailOff disables it.
// StatsDetailDump returns the current detail keys.
const (
	StatsDetailOn StatsDetailMode = iota + 1
	StatsDetailOff
	StatsDetailDump
)

// LRUMode is flat or segmented LRU. Other strings are rejected.
type LRUMode string

// LRUFlat and LRUSegmented are the accepted LRU modes.
const (
	LRUFlat      LRUMode = "flat"
	LRUSegmented LRUMode = "segmented"
)

// SlabsAutomoveMode is the slabs automove policy.
// The zero value is treated as SlabsAutomoveStandby (unset → default).
type SlabsAutomoveMode uint8

// SlabsAutomoveStandby, SlabsAutomoveNormal, and SlabsAutomoveAggressive
// are the slabs automove policies. Wire values are 0/1/2 (see wire()).
const (
	SlabsAutomoveStandby SlabsAutomoveMode = iota + 1
	SlabsAutomoveNormal
	SlabsAutomoveAggressive
)

// wire returns the memcached slabs automove integer (0=standby, 1=normal, 2=aggressive).
func (m SlabsAutomoveMode) wire() (int, error) {
	switch m {
	case 0, SlabsAutomoveStandby:
		return 0, nil
	case SlabsAutomoveNormal:
		return 1, nil
	case SlabsAutomoveAggressive:
		return 2, nil
	default:
		return 0, fmt.Errorf("slabs automove: %w", ErrInvalidArgument)
	}
}

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
	if c.closed.Load() {
		return nil, ErrClosed
	}
	return &Admin{c: c}, nil
}

// On pins admin commands to one configured server.
// The returned handle is never nil; methods return ErrAdminNotEnabled when unbound.
func (a *Admin) On(server Server) *AdminNode {
	if a == nil || a.c == nil {
		return &AdminNode{}
	}
	return &AdminNode{c: a.c, target: server}
}

// All returns a handle for Flush, Verbosity, and CacheMemLimit on every node.
// Shutdown is not available here; use On(server). Flush needs AllowFlushAll.
// The returned handle is never nil; methods return ErrAdminNotEnabled when unbound.
func (a *Admin) All() *AdminBroadcast {
	if a == nil || a.c == nil {
		return &AdminBroadcast{}
	}
	return &AdminBroadcast{c: a.c}
}

func (a *AdminNode) boundClient() (*Client, error) {
	if a == nil || a.c == nil {
		return nil, ErrAdminNotEnabled
	}
	return a.c, nil
}

// resolveTarget finds the configured member that matches the pinned AdminNode server.
// On success it also returns the bound client (never nil).
func (a *AdminNode) resolveTarget() (*addr, *Client, error) {
	c, err := a.boundClient()
	if err != nil {
		return nil, nil, err
	}
	if a.target.Address == "" {
		return nil, nil, ErrAdminTargetRequired
	}
	addrs, _ := c.snapshotCluster()
	wantNet := serverNetwork(a.target)
	for _, addr := range addrs {
		if addr != nil &&
			addr.Address == a.target.Address &&
			serverNetwork(addrToServer(addr)) == wantNet {
			return addr, c, nil
		}
	}
	return nil, nil, fmt.Errorf("admin target %s: %w", a.target.Address, ErrInvalidAddress)
}

// do dispatches a request/response admin command to the pinned node and maps the reply.
func (a *AdminNode) do(ctx context.Context, req *request, resp *response) error {
	if req == nil || resp == nil {
		return fmt.Errorf("admin request: %w", ErrInvalidArgument)
	}
	addr, c, err := a.resolveTarget()
	if err != nil {
		return err
	}
	resp.afterRecv = parseAdminReply
	if err := c.dispatchToAddr(ctx, addr, req, resp); err != nil {
		return fmt.Errorf("request: %w", err)
	}
	return nil
}

// doQuiet sends a noreply admin command to the pinned node.
func (a *AdminNode) doQuiet(ctx context.Context, req *request, resp *response) (err error) {
	start := time.Now()
	addr, c, err := a.resolveTarget()
	if err != nil {
		return err
	}
	defer func() { c.observeOp(ctx, start, opName(req), addr, 1, err) }()
	if err = c.sendQuietToAddr(ctx, addr, req, resp, false /* probeFault */); err != nil {
		return fmt.Errorf("request: %w", err)
	}
	return nil
}

// Verbosity sets the log level on the bound node.
func (a *AdminNode) Verbosity(ctx context.Context, level int, o AdminOpts) error {
	if level < 0 {
		return fmt.Errorf("verbosity: %w", ErrInvalidArgument)
	}
	req, resp, err := buildVerbosityCommand(level, o.Extra, o.Quiet)
	if err != nil {
		return err
	}
	defer releaseReqAndResp(req, resp)
	if o.Quiet {
		return a.doQuiet(ctx, req, resp)
	}
	return a.do(ctx, req, resp)
}

// ShutdownMode selects how the server stops.
type ShutdownMode int

const (
	// ShutdownImmediate stops the server without draining.
	ShutdownImmediate ShutdownMode = iota + 1
	// ShutdownGraceful asks the server to finish in-flight work first.
	ShutdownGraceful
)

// Shutdown tells the bound server to stop. On() is required.
func (a *AdminNode) Shutdown(ctx context.Context, mode ShutdownMode) error {
	addr, c, err := a.resolveTarget()
	if err != nil {
		return err
	}
	req, resp, err := buildShutdownCommand(mode == ShutdownGraceful)
	if err != nil {
		return err
	}
	defer releaseReqAndResp(req, resp)
	resp.afterRecv = parseShutdownReply
	err = c.dispatchToAddr(ctx, addr, req, resp)
	if err == nil || isShutdownClosed(err) {
		return nil
	}
	return fmt.Errorf("request: %w", err)
}

// CacheMemLimit sets the runtime cache size in megabytes.
func (a *AdminNode) CacheMemLimit(ctx context.Context, megabytes uint32, o AdminOpts) error {
	req, resp, err := buildCacheMemLimitCommand(megabytes, o.Extra, o.Quiet)
	if err != nil {
		return err
	}
	defer releaseReqAndResp(req, resp)
	if o.Quiet {
		return a.doQuiet(ctx, req, resp)
	}
	return a.do(ctx, req, resp)
}

// RefreshCerts asks a TLS-enabled server to reload its certificate chain/key.
// New connections use the refreshed material; existing sockets are not interrupted.
// Node-local only (On); replies map OK / ERROR like other admin verbs.
func (a *AdminNode) RefreshCerts(ctx context.Context) error {
	req, resp, err := buildRefreshCertsCommand()
	if err != nil {
		return err
	}
	defer releaseReqAndResp(req, resp)
	return a.do(ctx, req, resp)
}

// doAll runs a request/response admin command on every configured node.
func (b *AdminBroadcast) doAll(
	ctx context.Context,
	build func() (*request, *response),
) (err error) {
	if b == nil || b.c == nil {
		return ErrAdminNotEnabled
	}
	start := time.Now()
	sample, sampleResp := build()
	op := opName(sample)
	releaseReqAndResp(sample, sampleResp)
	addrs, _ := b.c.snapshotCluster()
	defer func() { b.c.observeOp(ctx, start, op, nil, len(addrs), err) }()
	if err = b.c.broadcastRequest(ctx, func(ctx context.Context, node *addr, h *connHolder) error {
		req, resp := build()
		defer releaseReqAndResp(req, resp)
		if err := b.c.sendOnConn(ctx, node, h, req, resp); err != nil {
			return err
		}
		return parseAdminReply(resp)
	}); err != nil {
		return fmt.Errorf("request: %w", err)
	}
	return nil
}

// doAllQuiet sends a noreply admin command to every configured node.
func (b *AdminBroadcast) doAllQuiet(
	ctx context.Context,
	build func() (*request, *response),
) (err error) {
	if b == nil || b.c == nil {
		return ErrAdminNotEnabled
	}
	start := time.Now()
	sample, sampleResp := build()
	op := opName(sample)
	releaseReqAndResp(sample, sampleResp)
	addrs, _ := b.c.snapshotCluster()
	defer func() { b.c.observeOp(ctx, start, op, nil, len(addrs), err) }()
	if len(addrs) == 1 {
		req, resp := build()
		defer releaseReqAndResp(req, resp)
		if req == nil || resp == nil {
			return fmt.Errorf("quiet request: %w", ErrInvalidArgument)
		}
		if err = b.c.sendQuietToAddr(ctx, addrs[0], req, resp, false /* probeFault */); err != nil {
			return fmt.Errorf("request: %w", err)
		}
		return nil
	}
	var (
		wg sync.WaitGroup
		mu sync.Mutex
	)
	errs := make([]error, 0, len(addrs))
	for _, addr := range addrs {
		wg.Go(func() {
			req, resp := build()
			defer releaseReqAndResp(req, resp)
			if e := b.c.sendQuietToAddr(ctx, addr, req, resp, false /* probeFault */); e != nil {
				mu.Lock()
				defer mu.Unlock()
				errs = append(errs, e)
			}
		})
	}
	wg.Wait()
	if len(errs) > 0 {
		return fmt.Errorf("request: %w", errors.Join(errs...))
	}
	return nil
}

// Verbosity sets the log level on every configured node.
func (b *AdminBroadcast) Verbosity(ctx context.Context, level int, o AdminOpts) error {
	if level < 0 {
		return fmt.Errorf("verbosity: %w", ErrInvalidArgument)
	}
	// doAll callbacks cannot return build errors; validate Extra here first.
	if err := validTextExtras(o.Extra); err != nil {
		return err
	}
	if o.Quiet {
		return b.doAllQuiet(ctx, func() (*request, *response) {
			req, resp, _ := buildVerbosityCommand(level, o.Extra, true /* noReply */)
			return req, resp
		})
	}
	return b.doAll(ctx, func() (*request, *response) {
		req, resp, _ := buildVerbosityCommand(level, o.Extra, false /* noReply */)
		return req, resp
	})
}

// CacheMemLimit sets the cache size on every configured node.
func (b *AdminBroadcast) CacheMemLimit(ctx context.Context, megabytes uint32, o AdminOpts) error {
	if err := validTextExtras(o.Extra); err != nil {
		return err
	}
	if o.Quiet {
		return b.doAllQuiet(ctx, func() (*request, *response) {
			req, resp, _ := buildCacheMemLimitCommand(megabytes, o.Extra, true /* noReply */)
			return req, resp
		})
	}
	return b.doAll(ctx, func() (*request, *response) {
		req, resp, _ := buildCacheMemLimitCommand(megabytes, o.Extra, false /* noReply */)
		return req, resp
	})
}
