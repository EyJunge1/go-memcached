package memcached

import (
	"context"
	"errors"
	"fmt"
	"time"
)

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
