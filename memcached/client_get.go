package memcached

import (
	"context"
	"fmt"
	"time"
)

// GetOpts chooses get, gets, gat, or gats.
type GetOpts struct {
	ReturnCAS bool     // ReturnCAS requests gets or gats (echo Unique; does not compare).
	Touch     *Exptime // Touch uses gat or gats; UDP returns ErrNotSupported.
	// Extra appends raw wire tokens after keys (escape hatch for new server options).
	Extra []string
}

// Get loads keys into dst. len(dst) must equal len(keys).
// dst[i] belongs to keys[i]. A miss leaves Found false; an empty hit is
// Found with len(Value)==0. Missing keys are not ErrNotFound.
//
// Keys must not be mutated during the call. Get does not copy keys into dst.
//
// If dst[i].Value has enough cap, the body is written there. Otherwise Get
// allocates. Reusing dst is the zero-allocation path.
func (c *Client) Get(
	ctx context.Context,
	keys [][]byte,
	dst []GetResult,
	opts GetOpts,
) (err error) {
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
	obs := c.observer()
	if obs != nil {
		start := time.Now()
		defer func() {
			hits, misses, respBytes := countGetOutcomes(dst)
			e := OpEvent{
				Op:            command,
				Duration:      time.Since(start),
				Err:           err,
				Keys:          len(keys),
				Hits:          hits,
				Misses:        misses,
				ResponseBytes: respBytes,
			}
			if len(keys) == 1 {
				e.Key = keys[0]
			}
			c.observeEvent(ctx, e)
		}()
	}
	return c.getFromNodes(ctx, command, exp, keys, dst, withCAS, opts.Extra)
}

// GetOne is Get for a single key. dst must be non-nil. Reusing dst is the
// zero-allocation path when dst.Value has enough cap.
func (c *Client) GetOne(
	ctx context.Context,
	key []byte,
	dst *GetResult,
	opts GetOpts,
) (err error) {
	if dst == nil {
		return fmt.Errorf("get dst: %w", ErrInvalidArgument)
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	command, _ := getCommand(opts)
	obs := c.observer()
	var start time.Time
	var node *addr
	if obs != nil {
		start = time.Now()
		defer func() {
			e := OpEvent{
				Op:       command,
				Server:   serverFromAddr(node),
				Duration: time.Since(start),
				Err:      err,
				Keys:     1,
				Key:      key,
			}
			if err == nil && dst != nil {
				if dst.Found {
					e.Hits = 1
					e.ResponseBytes = int64(len(dst.Value))
				} else {
					e.Misses = 1
				}
			}
			c.observeEvent(ctx, e)
		}()
	}

	wired, keyBox := c.nsKey(key)
	if err = validateKeyAndValue(wired, nil); err != nil {
		putBuilderBox(keyBox)
		return err
	}
	node, err = c.pickNode(wired)
	if err != nil {
		putBuilderBox(keyBox)
		return err
	}
	return c.getOneWired(ctx, node, wired, keyBox, dst, opts)
}

// getOneOnAddr is GetOne pinned to node (no observer; NodeClient path).
func (c *Client) getOneOnAddr(ctx context.Context, node *addr, key []byte, dst *GetResult, opts GetOpts) error {
	if dst == nil {
		return fmt.Errorf("get dst: %w", ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	wired, keyBox := c.nsKey(key)
	if err := validateKeyAndValue(wired, nil); err != nil {
		putBuilderBox(keyBox)
		return err
	}
	return c.getOneWired(ctx, node, wired, keyBox, dst, opts)
}

func (c *Client) getOneWired(
	ctx context.Context,
	node *addr,
	wired []byte,
	keyBox *[]byte,
	dst *GetResult,
	opts GetOpts,
) error {
	resetGetResult(dst)
	command, withCAS := getCommand(opts)
	var exp Exptime
	if opts.Touch != nil {
		exp = *opts.Touch
	}
	req, err := encodeGetOneCommand(command, exp, wired, opts.Extra)
	if err != nil {
		putBuilderBox(keyBox)
		return err
	}
	req.adoptKeyBox(keyBox)
	resp := buildRetrievalResponseOne(wired, dst, withCAS)
	defer releaseReqAndResp(req, resp)
	return c.dispatchToAddr(withObserveSkip(ctx), node, req, resp)
}

func (c *Client) hasNS() bool {
	return c != nil && c.rt != nil && c.rt.namespace != ""
}

// getFromAddr fetches keys from one node into dst using origIdx (nil = identity).
func (c *Client) getFromAddr(
	ctx context.Context,
	node *addr,
	command string,
	expiry Exptime,
	keys [][]byte,
	origIdx []int,
	dst []GetResult,
	withCAS bool,
	extra []string,
) error {
	var req *request
	var err error
	if command == "gats" || command == "gat" {
		req, err = encodeGetAndTouchesCommand(command, expiry, extra, keys...)
	} else {
		req, err = encodeGetsCommand(command, extra, keys...)
	}
	if err != nil {
		return err
	}
	resp := buildRetrievalResponse(keys, origIdx, dst, withCAS)
	defer releaseReqAndResp(req, resp)
	return c.dispatchToAddr(withObserveSkip(ctx), node, req, resp)
}

func countGetOutcomes(dst []GetResult) (hits, misses int, respBytes int64) {
	for i := range dst {
		if !dst[i].Found {
			misses++
			continue
		}
		hits++
		respBytes += int64(len(dst[i].Value))
	}
	return hits, misses, respBytes
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
