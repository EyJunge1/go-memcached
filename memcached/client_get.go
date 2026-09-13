package memcached

import (
	"context"
	"errors"
	"fmt"
)

// GetOpts chooses get, gets, gat, or gats.
type GetOpts struct {
	CAS   bool     // CAS requests gets or gats.
	Touch *Exptime // Touch uses gat or gats; UDP returns ErrNotSupported.
}

// Get loads keys. Misses are omitted and an empty slice is not ErrNotFound.
func (c *Client) Get(ctx context.Context, keys [][]byte, opts GetOpts) ([]Item, error) {
	if len(keys) == 0 {
		return []Item{}, nil
	}
	command, withCAS := getCommand(opts)
	var exp Exptime
	if opts.Touch != nil {
		exp = *opts.Touch
	}
	items, err := c.getFromNodes(ctx, command, exp, keys, withCAS)
	if c.hasNS() {
		for i := range items {
			items[i].Key = c.stripNS(items[i].Key)
		}
	}
	return items, err
}

func (c *Client) hasNS() bool {
	return c != nil && c.rt != nil && c.rt.namespace != ""
}

// getCommand selects get, gets, gat, or gats from GetOpts.
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

// getFromAddr fetches keys from one node and parses VALUE replies.
func (c *Client) getFromAddr(ctx context.Context, node *addr, command string, expiry Exptime, keys [][]byte, withCAS bool) ([]Item, error) {
	var req *request
	if command == "gats" || command == "gat" {
		req = encodeGetAndTouchesCommand(command, expiry, keys...)
	} else {
		req = encodeGetsCommand(command, keys...)
	}
	resp := buildRetrievalResponse(len(keys), withCAS)
	defer releaseReqAndResp(req, resp)

	if err := c.dispatchToAddr(ctx, node, req, resp); err != nil {
		return nil, err
	}
	items := resp.items
	resp.items = nil
	return items, nil
}

// getFromNodes groups keys by picker and fetches each node concurrently.
func (c *Client) getFromNodes(ctx context.Context, command string, expiry Exptime, keys [][]byte, withCAS bool) ([]Item, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return []Item{}, nil
	}

	addrs, picker := c.snapshotCluster()
	if picker == nil || len(addrs) == 0 {
		return nil, fmt.Errorf("no servers configured: %w", ErrInvalidAddress)
	}
	addrs = c.liveAddrs(addrs)

	wired, err := c.wireGetKeys(keys)
	if err != nil {
		return nil, err
	}

	if len(addrs) == 1 {
		return c.getFromAddr(ctx, addrs[0], command, expiry, wired, withCAS)
	}

	if len(wired) == 1 {
		addr, err := picker.Pick(addrs, wired[0])
		if err != nil {
			return nil, fmt.Errorf("pick node failed: %w", err)
		}
		return c.getFromAddr(ctx, addr, command, expiry, wired, withCAS)
	}

	first, err := picker.Pick(addrs, wired[0])
	if err != nil {
		return nil, fmt.Errorf("pick node failed: %w", err)
	}
	same := true
	for i := 1; i < len(wired); i++ {
		addr, err := picker.Pick(addrs, wired[i])
		if err != nil {
			return nil, fmt.Errorf("pick node failed: %w", err)
		}
		if addr != first {
			same = false
			break
		}
	}
	if same {
		return c.getFromAddr(ctx, first, command, expiry, wired, withCAS)
	}

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
		items []Item
		err   error
	}
	ch := make(chan nodeResult, len(groups))
	for target, nodeKeys := range groups {
		go func(node *addr, nodeKeys [][]byte) {
			items, err := c.getFromAddr(ctx, node, command, expiry, nodeKeys, withCAS)
			ch <- nodeResult{items: items, err: err}
		}(target, nodeKeys)
	}

	var items []Item
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

// wireGetKeys prefixes keys with the namespace when one is configured.
func (c *Client) wireGetKeys(keys [][]byte) ([][]byte, error) {
	if !c.hasNS() {
		for _, key := range keys {
			if err := validateKeyAndValue(key, nil); err != nil {
				return nil, err
			}
		}
		return keys, nil
	}
	wired := make([][]byte, len(keys))
	for i, key := range keys {
		wired[i] = c.nsBytes(key)
		if err := validateKeyAndValue(wired[i], nil); err != nil {
			return nil, err
		}
	}
	return wired, nil
}
