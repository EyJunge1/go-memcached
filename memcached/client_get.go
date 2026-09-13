package memcached

import (
	"context"
	"errors"
	"fmt"
)

// GetOpts selects get vs gets vs gat vs gats.
type GetOpts struct {
	CAS   bool     // gets / gats
	Touch *Exptime // gat / gats; TCP-only (UDP returns ErrNotSupported)
}

// Get fetches keys. Missing keys are omitted. An empty result is not ErrNotFound.
// Keys are grouped by cluster node and fetched in parallel.
// Item.Key is the wire key, including Config.Cluster.Namespace when set.
func (c *Client) Get(ctx context.Context, opts GetOpts, keys ...[]byte) ([]*Item, error) {
	if len(keys) == 0 {
		return []*Item{}, nil
	}
	command, withCAS := getCommand(opts)
	var exp Exptime
	if opts.Touch != nil {
		exp = *opts.Touch
	}
	return c.getFromNodes(ctx, command, exp, keys, withCAS)
}

func getCommand(opts GetOpts) (command string, withCAS bool) {
	if opts.Touch != nil {
		if opts.CAS {
			return "gats", true
		}
		return "gat", false
	}
	if opts.CAS {
		return "gets", true
	}
	return "get", false
}

func (c *Client) getFromAddr(ctx context.Context, node *addr, command string, expiry Exptime, keys [][]byte, withCAS bool) ([]*Item, error) {
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
	if err := c.dispatchToAddr(ctx, node, req, resp); err != nil {
		return nil, err
	}
	return items, nil
}

func (c *Client) getFromNodes(ctx context.Context, command string, expiry Exptime, keys [][]byte, withCAS bool) ([]*Item, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return []*Item{}, nil
	}

	wired := make([][]byte, len(keys))
	for i, key := range keys {
		wired[i] = c.nsBytes(key)
		if err := validateKeyAndValue(wired[i], nil); err != nil {
			return nil, err
		}
	}

	addrs, picker := c.snapshotCluster()
	if picker == nil || len(addrs) == 0 {
		return nil, fmt.Errorf("no servers configured: %w", ErrInvalidAddress)
	}
	addrs = c.liveAddrs(addrs)

	groups := make(map[*addr][][]byte, len(addrs))
	for _, key := range wired {
		addr, err := picker.Pick(addrs, key)
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
	for target, nodeKeys := range groups {
		go func(node *addr, nodeKeys [][]byte) {
			items, err := c.getFromAddr(ctx, node, command, expiry, nodeKeys, withCAS)
			ch <- nodeResult{items: items, err: err}
		}(target, nodeKeys)
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
