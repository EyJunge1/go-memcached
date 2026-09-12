package memcached

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
)

func (c *Client) getFromNodes(
	ctx context.Context,
	command string,
	expiry Exptime,
	keys [][]byte,
	dst []GetResult,
	withCAS bool,
	extra []string,
) error {
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
		return c.getFromAddr(ctx, addrs[0], command, expiry, wired, nil, dst, withCAS, extra)
	}

	if len(wired) == 1 {
		addr, err := picker.Pick(addrs, wired[0])
		if err != nil {
			return fmt.Errorf("pick node: %w", err)
		}
		return c.getFromAddr(ctx, addr, command, expiry, wired, nil, dst, withCAS, extra)
	}

	scratch := takeGetFanoutScratch(len(wired), len(addrs))
	defer putGetFanoutScratch(scratch)
	routed := scratch.routed
	same := true
	for i := range wired {
		nodeIndex, _, err := pickAddrIndex(picker, addrs, wired[i])
		if err != nil {
			return fmt.Errorf("pick node: %w", err)
		}
		routed[i] = nodeIndex
		if i > 0 && nodeIndex != routed[0] {
			same = false
		}
	}
	if same {
		return c.getFromAddr(ctx, addrs[routed[0]], command, expiry, wired, nil, dst, withCAS, extra)
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

	var mu sync.Mutex
	errs := make([]error, 0, activeGroups)
	if activeGroups == 1 {
		for i, node := range addrs {
			start, end := scratch.offsets[i], scratch.offsets[i+1]
			if start != end {
				return c.getFromAddr(
					ctx, node, command, expiry,
					scratch.keys[start:end], scratch.idx[start:end], dst, withCAS, extra,
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
			scratch.keys[start:end], scratch.idx[start:end], dst, withCAS, extra,
		); err != nil {
			mu.Lock()
			defer mu.Unlock()
			errs = append(errs, err)
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

const _maxPooledGetFanout = 1024

var _getFanoutPool = sync.Pool{
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
	s, ok := _getFanoutPool.Get().(*getFanoutScratch)
	if !ok || s == nil {
		s = &getFanoutScratch{
			routed:  make([]int, 0, 32),
			keys:    make([][]byte, 0, 32),
			idx:     make([]int, 0, 32),
			counts:  make([]int, 0, 4),
			offsets: make([]int, 0, 5),
		}
	}
	if cap(s.routed) < keys {
		s.routed = make([]int, keys)
		s.keys = make([][]byte, keys)
		s.idx = make([]int, keys)
	}
	s.routed = s.routed[:keys]
	s.keys = s.keys[:keys]
	s.idx = s.idx[:keys]
	if cap(s.counts) < nodes {
		s.counts = make([]int, nodes)
		s.offsets = make([]int, nodes+1)
	}
	s.counts = s.counts[:nodes]
	s.offsets = s.offsets[:nodes+1]
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
	if cap(s.routed) > _maxPooledGetFanout {
		return
	}
	s.routed = s.routed[:0]
	s.keys = s.keys[:0]
	s.idx = s.idx[:0]
	s.counts = s.counts[:0]
	s.offsets = s.offsets[:0]
	_getFanoutPool.Put(s)
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
