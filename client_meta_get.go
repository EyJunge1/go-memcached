package memcached

import (
	"context"
	"errors"
	"fmt"
	"strconv"
)

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
