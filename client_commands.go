package memcached

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ItemWriteOption customizes a single storage/mutation call.
type ItemWriteOption func(*itemWriteOpts)

type itemWriteOpts struct {
	noReply bool
}

// NoReply omits the server response for this call. Overrides the client-wide
// WithNoReply default when passed; without it the client default is used.
func NoReply() ItemWriteOption {
	return func(o *itemWriteOpts) { o.noReply = true }
}

func (c *Client) resolveNoReply(opts []ItemWriteOption) bool {
	o := itemWriteOpts{noReply: c.options.noReply}
	for _, fn := range opts {
		fn(&o)
	}
	return o.noReply
}

/**
 * Storage commands: set, add, replace, append, prepend, cas
 */

func (c *Client) storageCommand(ctx context.Context, command, key string, value []byte, flag uint32, expiry time.Duration, opts ...ItemWriteOption) error {
	key = c.nsString(key)
	if err := validateKeyAndValue([]byte(key), value); err != nil {
		return err
	}

	req, resp := buildStorageCommand(command, key, value, flag, expiry, c.resolveNoReply(opts))
	defer releaseReqAndResp(req, resp)

	resp.afterRecv = mapStorageReply
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return fmt.Errorf("request failed: %w", err)
	}

	return nil
}

// Set stores the given key-value pair.
func (c *Client) Set(ctx context.Context, key string, value []byte, flag uint32, expiry time.Duration, opts ...ItemWriteOption) error {
	return c.storageCommand(ctx, "set", key, value, flag, expiry, opts...)
}

// Add stores the pair only if the key does not exist.
func (c *Client) Add(ctx context.Context, key string, value []byte, flag uint32, expiry time.Duration, opts ...ItemWriteOption) error {
	return c.storageCommand(ctx, "add", key, value, flag, expiry, opts...)
}

// Replace updates an existing item.
func (c *Client) Replace(ctx context.Context, key string, value []byte, flag uint32, expiry time.Duration, opts ...ItemWriteOption) error {
	return c.storageCommand(ctx, "replace", key, value, flag, expiry, opts...)
}

// Append appends value to an existing item.
func (c *Client) Append(ctx context.Context, key string, value []byte, flag uint32, expiry time.Duration, opts ...ItemWriteOption) error {
	return c.storageCommand(ctx, "append", key, value, flag, expiry, opts...)
}

// Prepend prepends value to an existing item.
func (c *Client) Prepend(ctx context.Context, key string, value []byte, flag uint32, expiry time.Duration, opts ...ItemWriteOption) error {
	return c.storageCommand(ctx, "prepend", key, value, flag, expiry, opts...)
}

// Cas updates an existing item using check-and-set.
func (c *Client) Cas(ctx context.Context, key string, value []byte, flag uint32, expiry time.Duration, cas uint64, opts ...ItemWriteOption) error {
	key = c.nsString(key)
	if err := validateKeyAndValue([]byte(key), value); err != nil {
		return err
	}

	req, resp := buildCasCommand(key, value, flag, expiry, cas, c.resolveNoReply(opts))
	defer releaseReqAndResp(req, resp)

	resp.afterRecv = mapStorageReply
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return fmt.Errorf("request failed: %w", err)
	}

	return nil
}

/**
 * Retrieval commands: get, gets, gat, gats
 */

// Get returns the item for key. Use Gets if you need the CAS value.
func (c *Client) Get(ctx context.Context, key string) (*Item, error) {
	key = c.nsString(key)
	if err := validateKeyAndValue([]byte(key), nil); err != nil {
		return nil, err
	}

	req, resp := buildGetsCommand("get", key)
	defer releaseReqAndResp(req, resp)

	var item *Item
	resp.afterRecv = func(r *response) error {
		items, err := parseValueItems(r.rawLines, false, false)
		if err != nil {
			return fmt.Errorf("parse values failed: %w", err)
		}
		if len(items) == 0 {
			return fmt.Errorf("no items found: %w", ErrNotFound)
		}
		item = items[0]
		return nil
	}
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	return item, nil
}

// Gets returns items including CAS. Keys are grouped by cluster node and fetched in parallel.
func (c *Client) Gets(ctx context.Context, keys ...string) ([]*Item, error) {
	if len(keys) == 0 {
		return []*Item{}, nil
	}
	items, err := c.getFromNodes(ctx, "gets", 0, keys, true)
	return clusterGetItems(items, err)
}

// GetAndTouch returns the item and updates its expiration.
func (c *Client) GetAndTouch(ctx context.Context, expiry time.Duration, key string) (*Item, error) {
	key = c.nsString(key)
	if err := validateKeyAndValue([]byte(key), nil); err != nil {
		return nil, err
	}

	req, resp := buildGetAndTouchesCommand("gat", expiry, key)
	defer releaseReqAndResp(req, resp)

	var item *Item
	resp.afterRecv = func(r *response) error {
		items, err := parseValueItems(r.rawLines, false, false)
		if err != nil {
			return fmt.Errorf("parse values failed: %w", err)
		}
		if len(items) == 0 {
			return fmt.Errorf("no items found: %w", ErrNotFound)
		}
		item = items[0]
		return nil
	}
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	return item, nil
}

// GetAndTouches returns items, updates their expiration, and includes CAS.
// Keys are grouped by cluster node and fetched in parallel.
func (c *Client) GetAndTouches(ctx context.Context, expiry time.Duration, keys ...string) ([]*Item, error) {
	if len(keys) == 0 {
		return []*Item{}, nil
	}
	items, err := c.getFromNodes(ctx, "gats", expiry, keys, true)
	return clusterGetItems(items, err)
}

func clusterGetItems(items []*Item, err error) ([]*Item, error) {
	if len(items) == 0 {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("no items found: %w", ErrNotFound)
	}
	return items, err
}

// GetMulti fetches keys in parallel per cluster node. Missing keys are omitted.
// A non-nil error is only returned for I/O or parse failures.
func (c *Client) GetMulti(ctx context.Context, keys []string) (map[string]*Item, error) {
	items, err := c.getFromNodes(ctx, "gets", 0, keys, true)
	out := make(map[string]*Item, len(items))
	for _, item := range items {
		out[item.Key] = item
	}
	return out, err
}

func (c *Client) getFromAddr(ctx context.Context, addr *Addr, command string, expiry time.Duration, keys []string, withCAS bool) ([]*Item, error) {
	var req *request
	var resp *response
	if command == "gats" || command == "gat" {
		req, resp = buildGetAndTouchesCommand(command, expiry, keys...)
	} else {
		req, resp = buildGetsCommand(command, keys...)
	}
	defer releaseReqAndResp(req, resp)

	var items []*Item
	resp.afterRecv = func(r *response) error {
		parsed, err := parseValueItems(r.rawLines, false, withCAS)
		if err != nil {
			return fmt.Errorf("parse values failed: %w", err)
		}
		items = parsed
		return nil
	}
	if err := c.dispatchToAddr(ctx, addr, req, resp); err != nil {
		return nil, err
	}
	return items, nil
}

func (c *Client) getFromNodes(ctx context.Context, command string, expiry time.Duration, keys []string, withCAS bool) ([]*Item, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return []*Item{}, nil
	}

	wired := make([]string, len(keys))
	for i, key := range keys {
		wired[i] = c.nsString(key)
		if err := validateKeyAndValue([]byte(wired[i]), nil); err != nil {
			return nil, err
		}
	}

	addrs, picker := c.snapshotCluster()
	if picker == nil || len(addrs) == 0 {
		return nil, errors.New("no servers configured")
	}
	addrs = c.liveAddrs(addrs)

	groups := make(map[*Addr][]string, len(addrs))
	for _, key := range wired {
		addr, err := picker.Pick(addrs, []byte(command), c.hashKey([]byte(key)))
		if err != nil {
			return nil, fmt.Errorf("pick node failed: %w", err)
		}
		groups[addr] = append(groups[addr], key)
	}

	if len(groups) == 1 {
		for addr, nodeKeys := range groups {
			return c.getFromAddr(ctx, addr, command, expiry, nodeKeys, withCAS)
		}
	}

	type nodeResult struct {
		items []*Item
		err   error
	}
	ch := make(chan nodeResult, len(groups))
	for addr, nodeKeys := range groups {
		go func(addr *Addr, nodeKeys []string) {
			items, err := c.getFromAddr(ctx, addr, command, expiry, nodeKeys, withCAS)
			ch <- nodeResult{items: items, err: err}
		}(addr, nodeKeys)
	}

	var items []*Item
	var errs []error
	for range groups {
		result := <-ch
		items = append(items, result.items...)
		if result.err != nil {
			errs = append(errs, result.err)
		}
	}
	return items, errors.Join(errs...)
}

/**
 * Other commands: delete, incr, decr, touch, version, flush_all
 */

// Delete removes the key.
func (c *Client) Delete(ctx context.Context, key string, opts ...ItemWriteOption) error {
	key = c.nsString(key)
	if err := validateKeyAndValue([]byte(key), nil); err != nil {
		return err
	}

	req, resp := buildDeleteCommand(key, c.resolveNoReply(opts))
	defer releaseReqAndResp(req, resp)

	resp.afterRecv = expectLine(_DeletedCRLFBytes)
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return fmt.Errorf("request failed: %w", err)
	}

	return nil
}

// Incr increments the key. With no-reply it returns 0.
func (c *Client) Incr(ctx context.Context, key string, delta uint64, opts ...ItemWriteOption) (uint64, error) {
	key = c.nsString(key)
	if err := validateKeyAndValue([]byte(key), nil); err != nil {
		return 0, err
	}

	req, resp := buildArithmeticCommand("incr", key, delta, c.resolveNoReply(opts))
	defer releaseReqAndResp(req, resp)

	var value uint64
	resp.afterRecv = parseArithmeticAfterRecv(&value)
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return 0, fmt.Errorf("request failed: %w", err)
	}

	return value, nil
}

// Decr decrements the key. With no-reply it returns 0.
func (c *Client) Decr(ctx context.Context, key string, delta uint64, opts ...ItemWriteOption) (uint64, error) {
	key = c.nsString(key)
	if err := validateKeyAndValue([]byte(key), nil); err != nil {
		return 0, err
	}

	req, resp := buildArithmeticCommand("decr", key, delta, c.resolveNoReply(opts))
	defer releaseReqAndResp(req, resp)

	var value uint64
	resp.afterRecv = parseArithmeticAfterRecv(&value)
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return 0, fmt.Errorf("request failed: %w", err)
	}

	return value, nil
}

// Touch updates expiration without fetching the item.
func (c *Client) Touch(ctx context.Context, key string, expiry time.Duration, opts ...ItemWriteOption) error {
	key = c.nsString(key)
	if err := validateKeyAndValue([]byte(key), nil); err != nil {
		return err
	}

	req, resp := buildTouchCommand(key, expiry, c.resolveNoReply(opts))
	defer releaseReqAndResp(req, resp)

	resp.afterRecv = expectLine(_TouchedCRLFBytes)
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return fmt.Errorf("request failed: %w", err)
	}

	return nil
}

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

func parseVersionLine(lines [][]byte) (string, error) {
	if len(lines) == 0 {
		return "", fmt.Errorf("unexpected version: %w", ErrMalformedResponse)
	}
	line := trimCRLF(lines[0])
	if !bytes.HasPrefix(line, _VersionBytes) {
		return "", fmt.Errorf("%s: %w", string(line), ErrMalformedResponse)
	}
	return string(bytes.TrimSpace(line[len(_VersionBytes):])), nil
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

// FlushAll removes all items on every configured server immediately.
// It requires WithAllowFlushAll(); otherwise it returns ErrFlushAllNotAllowed
// and does not contact any node.
func (c *Client) FlushAll(ctx context.Context) error {
	return c.FlushAllAfter(ctx, 0)
}

// FlushAllAfter invalidates items after delay seconds (delay 0 is immediate).
// It requires WithAllowFlushAll().
func (c *Client) FlushAllAfter(ctx context.Context, delay time.Duration) error {
	return c.flushAllBroadcast(ctx, func(int) time.Duration { return delay })
}

// FlushAllStaggered sends flush_all with delay i*step on cluster node i
// so pool members do not expire together. Requires WithAllowFlushAll().
func (c *Client) FlushAllStaggered(ctx context.Context, step time.Duration) error {
	return c.flushAllBroadcast(ctx, func(i int) time.Duration { return time.Duration(i) * step })
}

func (c *Client) flushAllBroadcast(ctx context.Context, delayFor func(int) time.Duration) error {
	if !c.options.allowFlushAll {
		return ErrFlushAllNotAllowed
	}

	addrs, _ := c.snapshotCluster()
	var wg sync.WaitGroup
	errCh := make(chan error, len(addrs))
	for i, addr := range addrs {
		wg.Go(func() {
			errCh <- c.flushAllOnAddr(ctx, addr, delayFor(i))
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
		return fmt.Errorf("request failed: %w", errors.Join(errs...))
	}
	return nil
}

func (c *Client) flushAllOnAddr(ctx context.Context, addr *Addr, delay time.Duration) error {
	cn, err := c.getConn(ctx, addr)
	if err != nil {
		return err
	}
	var opErr error
	defer func() { finishConn(cn, opErr) }()

	req, resp := buildFlushAllCommand(delay, false)
	defer releaseReqAndResp(req, resp)
	c.enableUDPFraming(addr, req, resp)
	if err := req.send(ctx, cn, c.options.writeTimeout); err != nil {
		opErr = wrapTransport(fmt.Errorf("send failed: %w", err))
		return opErr
	}
	if err := resp.recv(ctx, cn, c.options.readTimeout); err != nil {
		opErr = wrapRecvError(fmt.Errorf("recv failed: %w", err))
		return opErr
	}
	if err := mapStatusLine(resp, _OKCRLFBytes); err != nil {
		opErr = wrapRecvError(err)
		return opErr
	}
	return nil
}

/**
 * Other commands(META text commands):
 * meta set(ms), meta get(mg), meta delete(md), meta arithmetic(ma), meta no-op(mn)
 */

// MetaSet stores a key with meta flags.
func (c *Client) MetaSet(ctx context.Context, key, value []byte, msOptions ...MetaSetOption) (*MetaItem, error) {
	msFlags := &metaSetFlags{}
	for _, applyFn := range msOptions {
		applyFn(msFlags)
	}
	key = c.nsBytes(key)
	if err := validateKeyAndValueFlags(key, value, msFlags.b); err != nil {
		return nil, err
	}
	if !validMetaSetMode(msFlags.M) {
		return nil, fmt.Errorf("invalid meta set mode: %w", ErrInvalidArgument)
	}
	if msFlags.OtokenSet {
		if err := validOpaqueToken(msFlags.Otoken); err != nil {
			return nil, err
		}
	}
	if msFlags.T > math.MaxInt64 {
		return nil, fmt.Errorf("TTL exceeds int64: %w", ErrInvalidArgument)
	}
	clientFlags := msFlags.F

	req, resp := buildMetaSetCommand(key, value, msFlags)
	defer releaseReqAndResp(req, resp)

	item := &MetaItem{
		Key:   key,
		TTL:   int64(msFlags.T),
		Flags: clientFlags,
	}
	resp.afterRecv = func(r *response) error {
		return parseMetaItem(r.rawLines, item, msFlags.q, false)
	}
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	return item, nil
}

// MetaGet fetches a key with meta flags.
func (c *Client) MetaGet(ctx context.Context, key []byte, mgOptions ...MetaGetOption) (*MetaItem, error) {
	mgFlags := &metaGetFlags{}
	for _, applyFn := range mgOptions {
		applyFn(mgFlags)
	}
	key = c.nsBytes(key)
	if err := validateKeyAndValueFlags(key, nil, mgFlags.b); err != nil {
		return nil, err
	}
	if mgFlags.OtokenSet {
		if err := validOpaqueToken(mgFlags.Otoken); err != nil {
			return nil, err
		}
	}

	req, resp := buildMetaGetCommand(key, mgFlags)
	defer releaseReqAndResp(req, resp)

	item := &MetaItem{
		Key: key,
	}
	resp.afterRecv = func(r *response) error {
		return parseMetaItem(r.rawLines, item, mgFlags.q, mgFlags.v)
	}
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	applyCheckCAS(item, mgFlags)

	return item, nil
}

func applyCheckCAS(item *MetaItem, flags *metaGetFlags) {
	if item == nil || flags == nil {
		return
	}
	item.CASMatched = flags.Cset && flags.v && item.gotHD && item.Value == nil
}

// MetaGetReq is one key plus options for MetaGetPipeline.
type MetaGetReq struct {
	Key     []byte
	Options []MetaGetOption
}

type preparedMetaGet struct {
	idx   int
	key   []byte
	flags *metaGetFlags
}

// MetaGetPipeline issues quiet meta-gets plus a mn barrier per node.
// Responses are correlated by opaque token (auto-assigned if missing).
func (c *Client) MetaGetPipeline(ctx context.Context, reqs []MetaGetReq) ([]*MetaItem, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(reqs) == 0 {
		return []*MetaItem{}, nil
	}

	preparedReqs := make([]preparedMetaGet, 0, len(reqs))
	out := make([]*MetaItem, len(reqs))
	for i, req := range reqs {
		flags := &metaGetFlags{}
		for _, applyFn := range req.Options {
			applyFn(flags)
		}
		req.Key = c.nsBytes(req.Key)
		if err := validateKeyAndValueFlags(req.Key, nil, flags.b); err != nil {
			return nil, err
		}
		if flags.OtokenSet {
			if err := validOpaqueToken(flags.Otoken); err != nil {
				return nil, err
			}
		} else if !flags.Oset {
			flags.Otoken = strconv.Itoa(i)
			flags.OtokenSet = true
		}
		// q hides HD; C(token) needs HD vs VA, so do not force quiet when checking CAS.
		if !flags.Cset {
			flags.q = true
		}
		preparedReqs = append(preparedReqs, preparedMetaGet{idx: i, key: req.Key, flags: flags})
		out[i] = &MetaItem{Key: req.Key}
	}

	addrs, picker := c.snapshotCluster()
	if picker == nil || len(addrs) == 0 {
		return nil, errors.New("no servers configured")
	}
	addrs = c.liveAddrs(addrs)

	groups := make(map[*Addr][]preparedMetaGet, len(addrs))
	for _, p := range preparedReqs {
		addr, err := picker.Pick(addrs, []byte("mg"), c.hashKey(p.key))
		if err != nil {
			return nil, fmt.Errorf("pick node failed: %w", err)
		}
		groups[addr] = append(groups[addr], p)
	}

	var errs []error
	for addr, group := range groups {
		if err := c.metaPipelineDispatch(ctx, addr, group, out); err != nil {
			errs = append(errs, err)
		}
	}
	return out, errors.Join(errs...)
}

func (c *Client) metaPipelineDispatch(ctx context.Context, addr *Addr, group []preparedMetaGet, out []*MetaItem) error {
	var raw []byte
	byOpaque := make(map[string]int, len(group))
	for _, p := range group {
		_, line := encodeMetaGetLine(p.key, p.flags)
		raw = append(raw, line...)
		tok := p.flags.Otoken
		if tok == "" {
			tok = strconv.FormatUint(p.flags.O, 10)
		}
		byOpaque[tok] = p.idx
	}
	raw = appendMetaQuietBarrier(raw)

	req := buildRequest([]byte("mg"), group[0].key, raw)
	resp := buildQuietBarrierResponse()
	defer releaseReqAndResp(req, resp)

	var parsed []*MetaItem
	resp.afterRecv = func(r *response) error {
		items, err := parseMetaPipeline(r.rawLines)
		parsed = items
		return err
	}
	if err := c.dispatchToAddr(ctx, addr, req, resp); err != nil {
		return err
	}
	for _, item := range parsed {
		idx, ok := byOpaque[item.OpaqueToken]
		if !ok && item.Opaque != 0 {
			idx, ok = byOpaque[strconv.FormatUint(item.Opaque, 10)]
		}
		if !ok {
			continue
		}
		if len(item.Key) == 0 {
			item.Key = out[idx].Key
		}
		for _, p := range group {
			if p.idx == idx {
				applyCheckCAS(item, p.flags)
				break
			}
		}
		out[idx] = item
	}
	return nil
}

// MetaDelete deletes a key with meta flags.
func (c *Client) MetaDelete(ctx context.Context, key []byte, options ...MetaDeleteOption) (*MetaItem, error) {
	mdFlags := &metaDeleteFlags{}
	for _, applyFn := range options {
		applyFn(mdFlags)
	}
	key = c.nsBytes(key)
	if err := validateKeyAndValueFlags(key, nil, mdFlags.b); err != nil {
		return nil, err
	}
	if mdFlags.OtokenSet {
		if err := validOpaqueToken(mdFlags.Otoken); err != nil {
			return nil, err
		}
	}

	req, resp := buildMetaDeleteCommand(key, mdFlags)
	defer releaseReqAndResp(req, resp)

	item := &MetaItem{
		Key: key,
	}
	resp.afterRecv = func(r *response) error {
		return parseMetaItem(r.rawLines, item, mdFlags.q, false)
	}
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	return item, nil
}

// MetaArithmetic increments or decrements a key with meta flags.
func (c *Client) MetaArithmetic(ctx context.Context, key []byte, delta uint64, options ...MetaArithmeticOption) (*MetaItem, error) {
	maFlags := &metaArithmeticFlags{}
	for _, applyFn := range options {
		applyFn(maFlags)
	}
	key = c.nsBytes(key)
	if err := validateKeyAndValueFlags(key, nil, maFlags.b); err != nil {
		return nil, err
	}
	if !validMetaArithmeticMode(maFlags.M) {
		return nil, fmt.Errorf("invalid meta arithmetic mode: %w", ErrInvalidArgument)
	}
	if maFlags.OtokenSet {
		if err := validOpaqueToken(maFlags.Otoken); err != nil {
			return nil, err
		}
	}

	req, resp := buildMetaArithmeticCommand(key, delta, maFlags)
	defer releaseReqAndResp(req, resp)

	item := &MetaItem{
		Key: key,
	}
	resp.afterRecv = func(r *response) error {
		return parseMetaItem(r.rawLines, item, maFlags.q, maFlags.v)
	}
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	return item, nil
}

func validateKeyAndValue(key, value []byte) error {
	return validateKeyAndValueFlags(key, value, false)
}

func validateKeyAndValueFlags(key, value []byte, binaryKey bool) error {
	nKey := len(key)
	if nKey == 0 {
		return ErrInvalidKey
	}
	if binaryKey {
		if base64.StdEncoding.EncodedLen(nKey) > maxKeySize {
			return ErrInvalidKey
		}
	} else if nKey > maxKeySize {
		return ErrInvalidKey
	}
	if !binaryKey {
		for _, b := range key {
			if b <= ' ' || b == 0x7f {
				return ErrInvalidKey
			}
		}
	}

	if nValue := len(value); nValue > maxValueSize {
		return ErrInvalidValue
	}

	return nil
}

// MetaDebug returns debug metadata for a key.
func (c *Client) MetaDebug(ctx context.Context, key []byte, options ...MetaDebugOption) (*MetaItemDebug, error) {
	mdFlags := &metaDebugFlags{}
	for _, applyFn := range options {
		applyFn(mdFlags)
	}
	key = c.nsBytes(key)
	if err := validateKeyAndValueFlags(key, nil, mdFlags.b); err != nil {
		return nil, err
	}

	req, resp := buildMetaDebugCommand(key, mdFlags)
	defer releaseReqAndResp(req, resp)

	item := &MetaItemDebug{
		Key: key,
	}
	resp.afterRecv = func(r *response) error {
		return parseMetaItemDebug(r.rawLines, item)
	}
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	return item, nil
}

// MetaNoop sends a meta no-op and expects MN.
func (c *Client) MetaNoop(ctx context.Context) error {
	req, resp := buildMetaNoopCommand()
	defer releaseReqAndResp(req, resp)

	resp.afterRecv = expectLine(_MetaMNCRLFBytes)
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return fmt.Errorf("request failed: %w", err)
	}

	return nil
}

/**
 * Other commands:
 * stats [subcommand]\r\n
 */

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

// Authenticate performs memcached token auth (-Y): a fake set whose body is
// "username password". Prefer TLS or a Unix socket; plaintext exposes the token.
func (c *Client) Authenticate(ctx context.Context, username, password string) error {
	if err := validAuthSecret(username); err != nil {
		return err
	}
	if err := validAuthSecret(password); err != nil {
		return err
	}
	req, resp := buildAuthCommand(username, password)
	defer releaseReqAndResp(req, resp)
	resp.afterRecv = mapStorageReply
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return fmt.Errorf("auth failed: %w", err)
	}
	return nil
}

func validAuthSecret(s string) error {
	for i := 0; i < len(s); i++ {
		if s[i] == '\r' || s[i] == '\n' || s[i] == 0 {
			return fmt.Errorf("auth credential invalid: %w", ErrInvalidArgument)
		}
	}
	return nil
}

func buildAuthCommand(username, password string) (*request, *response) {
	payload := []byte(username + " " + password)
	return buildStorageCommand("set", "_", payload, 0, 0, false)
}

func parseArithmeticAfterRecv(dst *uint64) func(*response) error {
	return func(resp *response) error {
		if resp.endIndicator == endIndicatorNoReply || len(resp.rawLines) == 0 {
			*dst = 0
			return nil
		}
		value, err := parseArithmetic(resp.rawLines[0])
		if err != nil {
			return fmt.Errorf("%s: %w", err.Error(), ErrMalformedResponse)
		}
		*dst = value
		return nil
	}
}
