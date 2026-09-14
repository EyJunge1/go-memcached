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

	wired, keyBox := c.nsKey(key)
	if err := validateKeyAndValue(wired, nil); err != nil {
		putBuilderBox(keyBox)
		return err
	}

	node, err := c.pickNode(wired)
	if err != nil {
		putBuilderBox(keyBox)
		return err
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

	scratch := takeGetFanoutScratch(len(wired), len(addrs))
	defer putGetFanoutScratch(scratch)
	routed := scratch.routed
	same := true
	for i := range wired {
		nodeIndex, _, err := pickAddrIndex(picker, addrs, wired[i])
		if err != nil {
			return fmt.Errorf("pick node failed: %w", err)
		}
		routed[i] = nodeIndex
		if i > 0 && nodeIndex != routed[0] {
			same = false
		}
	}
	if same {
		return c.getFromAddr(ctx, addrs[routed[0]], command, expiry, wired, nil, dst, withCAS)
	}

	for _, nodeIndex := range routed {
		scratch.counts[nodeIndex]++
	}
	activeGroups := 0
	for i := range addrs {
		scratch.offsets[i+1] = scratch.offsets[i] + scratch.counts[i]
		if scratch.counts[i] > 0 {
			activeGroups++
		}
		scratch.counts[i] = scratch.offsets[i]
	}
	for i, group := range routed {
		at := scratch.counts[group]
		scratch.keys[at] = wired[i]
		scratch.idx[at] = i
		scratch.counts[group]++
	}

	var (
		mu   sync.Mutex
		errs []error
	)
	if activeGroups == 1 {
		for i, node := range addrs {
			start, end := scratch.offsets[i], scratch.offsets[i+1]
			if start != end {
				return c.getFromAddr(
					ctx, node, command, expiry,
					scratch.keys[start:end], scratch.idx[start:end], dst, withCAS,
				)
			}
		}
	}
	n := 0
	for i := range addrs {
		if scratch.offsets[i] != scratch.offsets[i+1] {
			scratch.counts[n] = i
			n++
		}
	}
	c.runFanout(ctx, n, func(runCtx context.Context, group int) {
		i := scratch.counts[group]
		start, end := scratch.offsets[i], scratch.offsets[i+1]
		if err := c.getFromAddr(
			runCtx, addrs[i], command, expiry,
			scratch.keys[start:end], scratch.idx[start:end], dst, withCAS,
		); err != nil {
			mu.Lock()
			errs = append(errs, err)
			mu.Unlock()
		}
	})
	return errors.Join(errs...)
}

type getFanoutScratch struct {
	routed  []int
	keys    [][]byte
	idx     []int
	counts  []int
	offsets []int
}

const maxPooledGetFanout = 1024

var getFanoutPool = sync.Pool{
	New: func() any {
		return &getFanoutScratch{
			routed:  make([]int, 0, 32),
			keys:    make([][]byte, 0, 32),
			idx:     make([]int, 0, 32),
			counts:  make([]int, 0, 4),
			offsets: make([]int, 0, 5),
		}
	},
}

func takeGetFanoutScratch(keys, nodes int) *getFanoutScratch {
	s := getFanoutPool.Get().(*getFanoutScratch)
	if cap(s.routed) < keys {
		s.routed = make([]int, keys)
		s.keys = make([][]byte, keys)
		s.idx = make([]int, keys)
	} else {
		s.routed = s.routed[:keys]
		s.keys = s.keys[:keys]
		s.idx = s.idx[:keys]
	}
	if cap(s.counts) < nodes {
		s.counts = make([]int, nodes)
		s.offsets = make([]int, nodes+1)
	} else {
		s.counts = s.counts[:nodes]
		s.offsets = s.offsets[:nodes+1]
	}
	clear(s.counts)
	clear(s.offsets)
	return s
}

func putGetFanoutScratch(s *getFanoutScratch) {
	if s == nil {
		return
	}
	clear(s.routed)
	clear(s.keys)
	if cap(s.routed) > maxPooledGetFanout {
		return
	}
	s.routed = s.routed[:0]
	s.keys = s.keys[:0]
	s.idx = s.idx[:0]
	s.counts = s.counts[:0]
	s.offsets = s.offsets[:0]
	getFanoutPool.Put(s)
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
