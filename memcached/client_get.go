package memcached

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
)

// GetOpts chooses get, gets, gat, or gats.
type GetOpts struct {
	ReturnCAS bool     // ReturnCAS requests gets or gats (echo Unique; does not compare).
	Touch     *Exptime // Touch uses gat or gats; UDP returns ErrNotSupported.
}

// Get loads keys into dst. len(dst) must equal len(keys).
// dst[i] belongs to keys[i]. A miss leaves Found false; an empty hit is
// Found with len(Value)==0. Missing keys are not ErrNotFound.
//
// Keys must not be mutated during the call. Get does not copy keys into dst.
//
// If dst[i].Value has enough cap, the body is written there. Otherwise Get
// allocates. Reusing dst is the zero-allocation path.
func (c *Client) Get(ctx context.Context, keys [][]byte, dst []GetResult, opts GetOpts) error {
	if len(keys) != len(dst) {
		return fmt.Errorf("get dst length %d != keys %d: %w", len(dst), len(keys), ErrInvalidArgument)
	}
	if len(keys) == 0 {
		return nil
	}
	resetGetResults(dst)
	command, withCAS := getCommand(opts)
	var exp Exptime
	if opts.Touch != nil {
		exp = *opts.Touch
	}
	return c.getFromNodes(ctx, command, exp, keys, dst, withCAS)
}

// GetOne is Get for a single key. dst must be non-nil. Reusing dst is the
// zero-allocation path when dst.Value has enough cap.
func (c *Client) GetOne(ctx context.Context, key []byte, dst *GetResult, opts GetOpts) error {
	if dst == nil {
		return fmt.Errorf("get dst: %w", ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	resetGetResult(dst)
	command, withCAS := getCommand(opts)
	var exp Exptime
	if opts.Touch != nil {
		exp = *opts.Touch
	}

	addrs, picker := c.snapshotCluster()
	if picker == nil || len(addrs) == 0 {
		return fmt.Errorf("no servers configured: %w", ErrInvalidAddress)
	}
	addrs = c.liveAddrs(addrs)

	wired, keyBox := c.nsKey(key)
	if err := validateKeyAndValue(wired, nil); err != nil {
		putBuilderBox(keyBox)
		return err
	}

	var node *addr
	if len(addrs) == 1 {
		node = addrs[0]
	} else {
		picked, err := picker.Pick(addrs, wired)
		if err != nil {
			putBuilderBox(keyBox)
			return fmt.Errorf("pick node failed: %w", err)
		}
		node = picked
	}

	req := encodeGetOneCommand(command, exp, wired)
	req.adoptKeyBox(keyBox)
	resp := buildRetrievalResponseOne(wired, dst, withCAS)
	defer releaseReqAndResp(req, resp)
	return c.dispatchToAddr(ctx, node, req, resp)
}

func resetGetResults(dst []GetResult) {
	for i := range dst {
		resetGetResult(&dst[i])
	}
}

func resetGetResult(dst *GetResult) {
	dst.Found = false
	dst.Flags = 0
	dst.CAS = 0
	if dst.Value != nil {
		dst.Value = dst.Value[:0]
	}
}

func (c *Client) hasNS() bool {
	return c != nil && c.rt != nil && c.rt.namespace != ""
}

// getCommand selects get, gets, gat, or gats from GetOpts.
func getCommand(opts GetOpts) (command string, withCAS bool) {
	if opts.Touch != nil {
		if opts.ReturnCAS {
			return "gats", true
		}
		return "gat", false
	}
	if opts.ReturnCAS {
		return "gets", true
	}
	return "get", false
}

// getFromAddr fetches keys from one node into dst using origIdx (nil = identity).
func (c *Client) getFromAddr(ctx context.Context, node *addr, command string, expiry Exptime, keys [][]byte, origIdx []int, dst []GetResult, withCAS bool) error {
	var req *request
	if command == "gats" || command == "gat" {
		req = encodeGetAndTouchesCommand(command, expiry, keys...)
	} else {
		req = encodeGetsCommand(command, keys...)
	}
	resp := buildRetrievalResponse(keys, origIdx, dst, withCAS)
	defer releaseReqAndResp(req, resp)
	return c.dispatchToAddr(ctx, node, req, resp)
}

// getFromNodes groups keys by picker and fetches each node concurrently.
func (c *Client) getFromNodes(ctx context.Context, command string, expiry Exptime, keys [][]byte, dst []GetResult, withCAS bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	addrs, picker := c.snapshotCluster()
	if picker == nil || len(addrs) == 0 {
		return fmt.Errorf("no servers configured: %w", ErrInvalidAddress)
	}
	addrs = c.liveAddrs(addrs)

	wired, box, hdrp, err := c.wireGetKeys(keys)
	if err != nil {
		return err
	}
	defer releaseWiredGetKeys(box, hdrp)

	if len(addrs) == 1 {
		return c.getFromAddr(ctx, addrs[0], command, expiry, wired, nil, dst, withCAS)
	}

	if len(wired) == 1 {
		addr, err := picker.Pick(addrs, wired[0])
		if err != nil {
			return fmt.Errorf("pick node failed: %w", err)
		}
		return c.getFromAddr(ctx, addr, command, expiry, wired, nil, dst, withCAS)
	}

	first, err := picker.Pick(addrs, wired[0])
	if err != nil {
		return fmt.Errorf("pick node failed: %w", err)
	}
	same := true
	for i := 1; i < len(wired); i++ {
		addr, err := picker.Pick(addrs, wired[i])
		if err != nil {
			return fmt.Errorf("pick node failed: %w", err)
		}
		if addr != first {
			same = false
			break
		}
	}
	if same {
		return c.getFromAddr(ctx, first, command, expiry, wired, nil, dst, withCAS)
	}

	type getRef struct {
		key []byte
		idx int
	}
	groups := make(map[*addr][]getRef, len(addrs))
	for i, key := range wired {
		addr, err := picker.Pick(addrs, key)
		if err != nil {
			return fmt.Errorf("pick node failed: %w", err)
		}
		groups[addr] = append(groups[addr], getRef{key: key, idx: i})
	}

	if len(groups) == 1 {
		for addr := range groups {
			return c.getFromAddr(ctx, addr, command, expiry, wired, nil, dst, withCAS)
		}
	}

	var (
		mu   sync.Mutex
		errs []error
		wg   sync.WaitGroup
	)
	for addr, refs := range groups {
		nodeKeys := make([][]byte, len(refs))
		nodeIdx := make([]int, len(refs))
		for i, r := range refs {
			nodeKeys[i] = r.key
			nodeIdx[i] = r.idx
		}
		wg.Go(func() {
			if err := c.getFromAddr(ctx, addr, command, expiry, nodeKeys, nodeIdx, dst, withCAS); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	return errors.Join(errs...)
}

func matchRetrievalKey(keys [][]byte, want []byte, cursor *int) (int, bool) {
	for i := *cursor; i < len(keys); i++ {
		if bytes.Equal(keys[i], want) {
			*cursor = i + 1
			return i, true
		}
	}
	for i := 0; i < *cursor && i < len(keys); i++ {
		if bytes.Equal(keys[i], want) {
			return i, true
		}
	}
	return 0, false
}
