package memcached

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"time"
)

/**
 * Storage commands: set, add, replace, append, prepend, cas
 */

func (c *Client) storageCommand(ctx context.Context, command, key string, value []byte, flag uint32, expiry time.Duration) error {
	if err := validateKeyAndValue([]byte(key), value); err != nil {
		return err
	}

	req, resp := buildStorageCommand(command, key, value, flag, expiry, c.options.noReply)
	defer releaseReqAndResp(req, resp)

	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return fmt.Errorf("request failed: %w", err)
	}

	if err := resp.expect(_StoredCRLFBytes); err != nil {
		return fmt.Errorf("%s: %w", err.Error(), ErrMalformedResponse)
	}

	return nil
}

// Set stores the given key-value pair.
func (c *Client) Set(ctx context.Context, key string, value []byte, flag uint32, expiry time.Duration) error {
	return c.storageCommand(ctx, "set", key, value, flag, expiry)
}

// Add stores the pair only if the key does not exist.
func (c *Client) Add(ctx context.Context, key string, value []byte, flag uint32, expiry time.Duration) error {
	return c.storageCommand(ctx, "add", key, value, flag, expiry)
}

// Replace updates an existing item.
func (c *Client) Replace(ctx context.Context, key string, value []byte, flag uint32, expiry time.Duration) error {
	return c.storageCommand(ctx, "replace", key, value, flag, expiry)
}

// Append appends value to an existing item.
func (c *Client) Append(ctx context.Context, key string, value []byte, flag uint32, expiry time.Duration) error {
	return c.storageCommand(ctx, "append", key, value, flag, expiry)
}

// Prepend prepends value to an existing item.
func (c *Client) Prepend(ctx context.Context, key string, value []byte, flag uint32, expiry time.Duration) error {
	return c.storageCommand(ctx, "prepend", key, value, flag, expiry)
}

// Cas updates an existing item using check-and-set.
func (c *Client) Cas(ctx context.Context, key string, value []byte, flag uint32, expiry time.Duration, cas uint64) error {
	if err := validateKeyAndValue([]byte(key), value); err != nil {
		return err
	}

	req, resp := buildCasCommand(key, value, flag, expiry, cas, c.options.noReply)
	defer releaseReqAndResp(req, resp)

	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return fmt.Errorf("request failed: %w", err)
	}

	if err := resp.expect(_StoredCRLFBytes); err != nil {
		return fmt.Errorf("%s: %w", err.Error(), ErrMalformedResponse)
	}

	return nil
}

/**
 * Retrieval commands: get, gets, gat, gats
 */

// Get returns the item for key. Use Gets if you need the CAS value.
func (c *Client) Get(ctx context.Context, key string) (*Item, error) {
	if err := validateKeyAndValue([]byte(key), nil); err != nil {
		return nil, err
	}

	req, resp := buildGetsCommand("get", key)
	defer releaseReqAndResp(req, resp)

	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	items, err := parseValueItems(resp.rawLines, false, false)
	if err != nil {
		return nil, fmt.Errorf("parse values failed: %w", err)
	}
	if len(items) == 0 {
		return nil, fmt.Errorf("no items found: %w", ErrNotFound)
	}

	return items[0], nil
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
	if err := validateKeyAndValue([]byte(key), nil); err != nil {
		return nil, err
	}

	req, resp := buildGetAndTouchesCommand("gat", expiry, key)
	defer releaseReqAndResp(req, resp)

	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	items, err := parseValueItems(resp.rawLines, false, false)
	if err != nil {
		return nil, fmt.Errorf("parse values failed: %w", ErrMalformedResponse)
	}

	if len(items) == 0 {
		return nil, fmt.Errorf("no items found: %w", ErrNotFound)
	}

	return items[0], nil
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

	if err := c.dispatchToAddr(ctx, addr, req, resp); err != nil {
		return nil, err
	}
	items, err := parseValueItems(resp.rawLines, false, withCAS)
	if err != nil {
		return nil, fmt.Errorf("parse values failed: %w", ErrMalformedResponse)
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

	for _, key := range keys {
		if err := validateKeyAndValue([]byte(key), nil); err != nil {
			return nil, err
		}
	}

	addrs, picker := c.snapshotCluster()
	if picker == nil || len(addrs) == 0 {
		return nil, errors.New("no servers configured")
	}

	groups := make(map[*Addr][]string, len(addrs))
	for _, key := range keys {
		addr, err := picker.Pick(addrs, []byte(command), []byte(key))
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
func (c *Client) Delete(ctx context.Context, key string) error {
	if err := validateKeyAndValue([]byte(key), nil); err != nil {
		return err
	}

	req, resp := buildDeleteCommand(key, c.options.noReply)
	defer releaseReqAndResp(req, resp)

	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return fmt.Errorf("request failed: %w", err)
	}

	// expect DELETED\r\n
	if err := resp.expect(_DeletedCRLFBytes); err != nil {
		return fmt.Errorf("%s: %w", err.Error(), ErrMalformedResponse)
	}

	return nil
}

// Incr increments the key. With no-reply it returns 0.
func (c *Client) Incr(ctx context.Context, key string, delta uint64) (uint64, error) {
	if err := validateKeyAndValue([]byte(key), nil); err != nil {
		return 0, err
	}

	req, resp := buildArithmeticCommand("incr", key, delta, c.options.noReply)
	defer releaseReqAndResp(req, resp)

	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return 0, fmt.Errorf("request failed: %w", err)
	}

	// parse response
	value, err := parseArithmetic(resp.rawLines[0])
	if err != nil {
		return 0, fmt.Errorf("%s: %w", err.Error(), ErrMalformedResponse)
	}

	return value, nil
}

// Decr decrements the key. With no-reply it returns 0.
func (c *Client) Decr(ctx context.Context, key string, delta uint64) (uint64, error) {
	if err := validateKeyAndValue([]byte(key), nil); err != nil {
		return 0, err
	}

	req, resp := buildArithmeticCommand("decr", key, delta, c.options.noReply)
	defer releaseReqAndResp(req, resp)

	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return 0, fmt.Errorf("request failed: %w", err)
	}

	// parse response
	value, err := parseArithmetic(resp.rawLines[0])
	if err != nil {
		return 0, fmt.Errorf("%s: %w", err.Error(), ErrMalformedResponse)
	}

	return value, nil
}

// Touch updates expiration without fetching the item.
func (c *Client) Touch(ctx context.Context, key string, expiry time.Duration) error {
	if err := validateKeyAndValue([]byte(key), nil); err != nil {
		return err
	}

	req, resp := buildTouchCommand(key, expiry, c.options.noReply)
	defer releaseReqAndResp(req, resp)

	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return fmt.Errorf("request failed: %w", err)
	}

	// expect TOUCHED\r\n
	if err := resp.expect(_TouchedCRLFBytes); err != nil {
		return fmt.Errorf("%s: %w", err.Error(), ErrMalformedResponse)
	}

	return nil
}

// Ping sends version to every configured server and fails if any node is down.
func (c *Client) Ping(ctx context.Context) error {
	call := func(ctx context.Context, cn memcachedConn) error {
		req, resp := buildVersionCommand()
		defer releaseReqAndResp(req, resp)

		c.autoSwitchToUDP(ctx, req, resp)

		if err := req.send(ctx, cn, c.options.writeTimeout); err != nil {
			return wrapTransport(fmt.Errorf("send failed: %w", err))
		}
		if err := resp.recv(ctx, cn, c.options.readTimeout); err != nil {
			return wrapTransport(fmt.Errorf("recv failed: %w", err))
		}
		if len(resp.rawLines) == 0 || !bytes.HasPrefix(resp.rawLines[0], _VersionBytes) {
			return fmt.Errorf("unexpected ping response: %w", ErrMalformedResponse)
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

	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return "", fmt.Errorf("request: %w", err)
	}

	// parse version number from response
	// VERSION 1.6.14
	line := resp.rawLines[0]
	if !bytes.HasPrefix(line, _VersionBytes) {
		return "", fmt.Errorf("%s: %w", string(line), ErrMalformedResponse)
	}

	return string(trimCRLF(line[8:])), nil
}

// FlushAll removes all items on every configured server.
func (c *Client) FlushAll(ctx context.Context) error {
	call := func(ctx context.Context, cn memcachedConn) error {
		req, resp := buildFlushAllCommand(c.options.noReply)
		defer releaseReqAndResp(req, resp)

		c.autoSwitchToUDP(ctx, req, resp)

		if err := req.send(ctx, cn, c.options.writeTimeout); err != nil {
			return wrapTransport(fmt.Errorf("send failed: %w", err))
		}
		if err := resp.recv(ctx, cn, c.options.readTimeout); err != nil {
			return wrapTransport(fmt.Errorf("recv failed: %w", err))
		}

		// expect OK\r\n
		if err := resp.expect(_OKCRLFBytes); err != nil {
			return fmt.Errorf("%s: %w", err.Error(), ErrMalformedResponse)
		}

		return nil
	}

	if err := c.broadcastRequest(ctx, call); err != nil {
		return fmt.Errorf("request failed: %w", err)
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
	if err := validateKeyAndValueFlags(key, value, msFlags.b); err != nil {
		return nil, err
	}
	if msFlags.T > math.MaxInt64 {
		return nil, fmt.Errorf("TTL exceeds int64: %w", ErrInvalidArgument)
	}
	clientFlags := msFlags.F

	req, resp := buildMetaSetCommand(key, value, msFlags)
	defer releaseReqAndResp(req, resp)
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	item := &MetaItem{
		Key:   key,
		TTL:   int64(msFlags.T),
		Flags: clientFlags,
	}
	err := parseMetaItem(resp.rawLines, item, msFlags.q)
	if err != nil {
		return nil, err
	}

	return item, nil
}

// MetaGet fetches a key with meta flags.
func (c *Client) MetaGet(ctx context.Context, key []byte, mgOptions ...MetaGetOption) (*MetaItem, error) {
	mgFlags := &metaGetFlags{}
	for _, applyFn := range mgOptions {
		applyFn(mgFlags)
	}
	if err := validateKeyAndValueFlags(key, nil, mgFlags.b); err != nil {
		return nil, err
	}

	req, resp := buildMetaGetCommand(key, mgFlags)
	defer releaseReqAndResp(req, resp)

	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	item := &MetaItem{
		Key: key,
	}
	if err := parseMetaItem(resp.rawLines, item, mgFlags.q); err != nil {
		return nil, err
	}

	return item, nil
}

// MetaDelete deletes a key with meta flags.
func (c *Client) MetaDelete(ctx context.Context, key []byte, options ...MetaDeleteOption) (*MetaItem, error) {
	mdFlags := &metaDeleteFlags{}
	for _, applyFn := range options {
		applyFn(mdFlags)
	}
	if err := validateKeyAndValueFlags(key, nil, mdFlags.b); err != nil {
		return nil, err
	}

	req, resp := buildMetaDeleteCommand(key, mdFlags)
	defer releaseReqAndResp(req, resp)

	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	item := &MetaItem{
		Key: key,
	}
	if err := parseMetaItem(resp.rawLines, item, mdFlags.q); err != nil {
		return nil, err
	}

	return item, nil
}

// MetaArithmetic increments or decrements a key with meta flags.
func (c *Client) MetaArithmetic(ctx context.Context, key []byte, delta uint64, options ...MetaArithmeticOption) (*MetaItem, error) {
	maFlags := &metaArithmeticFlags{}
	for _, applyFn := range options {
		applyFn(maFlags)
	}
	if err := validateKeyAndValueFlags(key, nil, maFlags.b); err != nil {
		return nil, err
	}

	req, resp := buildMetaArithmeticCommand(key, delta, maFlags)
	defer releaseReqAndResp(req, resp)

	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	item := &MetaItem{
		Key: key,
	}
	if err := parseMetaItem(resp.rawLines, item, maFlags.q); err != nil {
		return nil, err
	}

	return item, nil
}

func validateKeyAndValue(key, value []byte) error {
	return validateKeyAndValueFlags(key, value, false)
}

func validateKeyAndValueFlags(key, value []byte, binaryKey bool) error {
	if nKey := len(key); nKey == 0 || nKey > maxKeySize {
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
	if err := validateKeyAndValueFlags(key, nil, mdFlags.b); err != nil {
		return nil, err
	}

	req, resp := buildMetaDebugCommand(key, mdFlags)
	defer releaseReqAndResp(req, resp)

	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	item := &MetaItemDebug{
		Key: key,
	}
	// parse response
	if err := parseMetaItemDebug(resp.rawLines, item); err != nil {
		return nil, err
	}

	return item, nil
}

// MetaNoop sends a meta no-op and expects MN.
func (c *Client) MetaNoop(ctx context.Context) error {
	req, resp := buildMetaNoopCommand()
	defer releaseReqAndResp(req, resp)

	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	if err := resp.expect(_MetaMNCRLFBytes); err != nil {
		if errors.Is(err, ErrMalformedResponse) {
			return err
		}

		return fmt.Errorf("%s: %w", err.Error(), ErrMalformedResponse)
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

	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	return parseStats(resp.rawLines)
}
