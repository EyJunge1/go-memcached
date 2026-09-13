package memcached

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
)

// stripTrailingMN removes a trailing mn\r\n so a pipelined command can share one barrier.
func stripTrailingMN(raw []byte) []byte {
	return bytes.TrimSuffix(raw, []byte("mn\r\n"))
}

// Get sends mg for key using flags f.
func (m *MetaClient) Get(ctx context.Context, key []byte, f MetaGet) (MetaResult, error) {
	c := m.c
	if err := f.validate(); err != nil {
		return MetaResult{}, err
	}
	mgFlags := f.toFlags()
	key = c.nsBytes(key)
	if err := c.checkKeyValueFlags(key, nil, mgFlags.b); err != nil {
		return MetaResult{}, err
	}
	if mgFlags.OtokenSet {
		if err := validOpaqueToken(mgFlags.Otoken); err != nil {
			return MetaResult{}, err
		}
	}

	req, resp := buildMetaGetCommand(key, mgFlags)
	defer releaseReqAndResp(req, resp)

	item := &MetaItem{Key: key}
	resp.afterRecv = func(r *response) error {
		return parseMetaItem(r.rawLines, item, mgFlags.q, true)
	}
	res, err := finishMeta(item, c.dispatchRequest(ctx, req, resp))
	if err != nil {
		return MetaResult{}, fmt.Errorf("request failed: %w", err)
	}
	applyCheckCAS(item, mgFlags)
	res.Item.CASMatched = item.CASMatched
	return res, nil
}

// applyCheckCAS sets CASMatched when mg used C and the server returned HD without a value.
func applyCheckCAS(item *MetaItem, flags *metaGetFlags) {
	if item == nil || flags == nil {
		return
	}
	item.CASMatched = flags.Cset && flags.v && item.gotHD && item.Value == nil
}

// preparedMetaOp is one encoded Meta().Pipeline step plus its opaque token.
type preparedMetaOp struct {
	idx      int
	key      []byte
	cmd      []byte
	line     []byte
	tok      string
	getFlags *metaGetFlags
	noop     bool
	barrier  MetaNoop
}

// metaOpKinds counts how many of Get, Set, Del, Arith, and Noop are set on op.
func metaOpKinds(op MetaOp) int {
	n := 0
	if op.Get != nil {
		n++
	}
	if op.Set != nil {
		n++
	}
	if op.Del != nil {
		n++
	}
	if op.Arith != nil {
		n++
	}
	if op.Noop != nil {
		n++
	}
	return n
}

// encodeMetaPipelineOp validates one MetaOp and encodes its quiet wire line for Pipeline.
func encodeMetaPipelineOp(c *Client, op MetaOp, idx int) (preparedMetaOp, error) {
	if metaOpKinds(op) != 1 {
		return preparedMetaOp{}, fmt.Errorf("meta op: %w", ErrInvalidArgument)
	}
	key := c.nsBytes(op.Key)
	prep := preparedMetaOp{idx: idx, key: key}

	switch {
	case op.Get != nil:
		f := *op.Get
		if err := f.validate(); err != nil {
			return preparedMetaOp{}, err
		}
		mg := f.toFlags()
		if !mg.Cset {
			mg.q = true
		}
		if err := c.checkKeyValueFlags(key, nil, mg.b); err != nil {
			return preparedMetaOp{}, err
		}
		if err := ensurePipelineOpaque(mg, idx); err != nil {
			return preparedMetaOp{}, err
		}
		_, line := encodeMetaGetLine(key, mg)
		prep.cmd = []byte("mg")
		prep.line = line
		prep.tok = opaqueToken(mg.Otoken, mg.O, mg.Oset || mg.OtokenSet)
		prep.getFlags = mg
	case op.Set != nil:
		f := *op.Set
		ms := f.toFlags()
		ms.q = true
		if err := c.checkKeyValueFlags(key, op.Body, ms.b); err != nil {
			return preparedMetaOp{}, err
		}
		if err := f.validate(); err != nil {
			return preparedMetaOp{}, err
		}
		if !validMetaSetMode(ms.M) {
			return preparedMetaOp{}, fmt.Errorf("invalid meta set mode: %w", ErrInvalidArgument)
		}
		if err := ensureSetOpaque(ms, idx); err != nil {
			return preparedMetaOp{}, err
		}
		req, resp := buildMetaSetCommand(key, op.Body, ms)
		line := stripTrailingMN(append([]byte(nil), req.wire()...))
		releaseReqAndResp(req, resp)
		prep.cmd = []byte("ms")
		prep.line = line
		prep.tok = opaqueToken(ms.Otoken, ms.O, ms.Oset || ms.OtokenSet)
		prep.key = key
	case op.Del != nil:
		f := *op.Del
		if err := f.validate(); err != nil {
			return preparedMetaOp{}, err
		}
		md := f.toFlags()
		md.q = true
		if err := c.checkKeyValueFlags(key, nil, md.b); err != nil {
			return preparedMetaOp{}, err
		}
		if err := ensureDeleteOpaque(md, idx); err != nil {
			return preparedMetaOp{}, err
		}
		req, resp := buildMetaDeleteCommand(key, md)
		line := stripTrailingMN(append([]byte(nil), req.raw...))
		releaseReqAndResp(req, resp)
		prep.cmd = []byte("md")
		prep.line = line
		prep.tok = opaqueToken(md.Otoken, md.O, md.Oset || md.OtokenSet)
	case op.Arith != nil:
		f := *op.Arith
		if err := f.validate(); err != nil {
			return preparedMetaOp{}, err
		}
		ma := f.toFlags()
		if !ma.v {
			ma.q = true
		}
		if err := c.checkKeyValueFlags(key, nil, ma.b); err != nil {
			return preparedMetaOp{}, err
		}
		if !validMetaArithmeticMode(ma.M) {
			return preparedMetaOp{}, fmt.Errorf("invalid meta arithmetic mode: %w", ErrInvalidArgument)
		}
		if err := ensureArithOpaque(ma, idx); err != nil {
			return preparedMetaOp{}, err
		}
		req, resp := buildMetaArithmeticCommand(key, op.Delta, ma)
		line := stripTrailingMN(append([]byte(nil), req.raw...))
		releaseReqAndResp(req, resp)
		prep.cmd = []byte("ma")
		prep.line = line
		prep.tok = opaqueToken(ma.Otoken, ma.O, ma.Oset || ma.OtokenSet)
	case op.Noop != nil:
		// The batch already ends with mn; another mn would make the barrier reader stop early.
		f := *op.Noop
		if err := f.validate(); err != nil {
			return preparedMetaOp{}, err
		}
		prep.cmd = []byte("mn")
		prep.line = nil
		prep.noop = true
		prep.barrier = f
	}
	return prep, nil
}

// opaqueToken returns the O token string used to correlate a pipelined reply.
func opaqueToken(tok string, num uint64, set bool) string {
	if tok != "" {
		return tok
	}
	if set {
		return strconv.FormatUint(num, 10)
	}
	return ""
}

// ensurePipelineOpaque assigns a unique O token for a pipelined mg when the caller omitted one.
func ensurePipelineOpaque(flags *metaGetFlags, idx int) error {
	if flags.OtokenSet {
		return validOpaqueToken(flags.Otoken)
	}
	if flags.Oset {
		return nil
	}
	flags.Otoken = strconv.Itoa(idx)
	flags.OtokenSet = true
	return nil
}

// ensureSetOpaque assigns a unique O token for a pipelined ms when the caller omitted one.
func ensureSetOpaque(flags *metaSetFlags, idx int) error {
	if flags.OtokenSet {
		return validOpaqueToken(flags.Otoken)
	}
	if flags.Oset {
		return nil
	}
	flags.Otoken = strconv.Itoa(idx)
	flags.OtokenSet = true
	return nil
}

// ensureDeleteOpaque assigns a unique O token for a pipelined md when the caller omitted one.
func ensureDeleteOpaque(flags *metaDeleteFlags, idx int) error {
	if flags.OtokenSet {
		return validOpaqueToken(flags.Otoken)
	}
	if flags.Oset {
		return nil
	}
	flags.Otoken = strconv.Itoa(idx)
	flags.OtokenSet = true
	return nil
}

// ensureArithOpaque assigns a unique O token for a pipelined ma when the caller omitted one.
func ensureArithOpaque(flags *metaArithmeticFlags, idx int) error {
	if flags.OtokenSet {
		return validOpaqueToken(flags.Otoken)
	}
	if flags.Oset {
		return nil
	}
	flags.Otoken = strconv.Itoa(idx)
	flags.OtokenSet = true
	return nil
}

// Pipeline runs mixed meta ops, assigns O when missing, appends mn, and fans out by node.
func (m *MetaClient) Pipeline(ctx context.Context, ops []MetaOp) ([]MetaResult, error) {
	c := m.c
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(ops) == 0 {
		return []MetaResult{}, nil
	}

	prepared := make([]preparedMetaOp, 0, len(ops))
	out := make([]MetaResult, len(ops))
	for i, op := range ops {
		p, err := encodeMetaPipelineOp(c, op, i)
		if err != nil {
			return nil, err
		}
		prepared = append(prepared, p)
		out[i] = MetaResult{Item: MetaItem{Key: p.key}}
	}

	addrs, picker := c.snapshotCluster()
	if picker == nil || len(addrs) == 0 {
		return nil, fmt.Errorf("no servers configured: %w", ErrInvalidAddress)
	}
	addrs = c.liveAddrs(addrs)

	groups := make(map[*addr][]preparedMetaOp, len(addrs))
	for _, p := range prepared {
		addr, err := picker.Pick(addrs, p.key)
		if err != nil {
			return nil, fmt.Errorf("pick node failed: %w", err)
		}
		groups[addr] = append(groups[addr], p)
	}

	if len(groups) == 1 {
		for addr, group := range groups {
			if err := c.metaPipelineDispatch(ctx, addr, group, out); err != nil {
				return out, err
			}
			return out, nil
		}
	}

	var (
		mu   sync.Mutex
		errs []error
		wg   sync.WaitGroup
	)
	for addr, group := range groups {
		wg.Go(func() {
			if err := c.metaPipelineDispatch(ctx, addr, group, out); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	return out, errors.Join(errs...)
}

// metaPipelineDispatch writes a node's quiet meta batch, then maps replies onto out by opaque.
func (c *Client) metaPipelineDispatch(ctx context.Context, node *addr, group []preparedMetaOp, out []MetaResult) error {
	var raw []byte
	var barrier MetaNoop
	byOpaque := make(map[string]int, len(group))
	for _, p := range group {
		raw = append(raw, p.line...)
		if p.noop {
			barrier = p.barrier
		}
		if p.tok == "" {
			continue
		}
		if _, exists := byOpaque[p.tok]; exists {
			return fmt.Errorf("duplicate opaque %q: %w", p.tok, ErrInvalidArgument)
		}
		byOpaque[p.tok] = p.idx
	}
	raw = appendMetaQuietBarrier(raw, barrier)

	req := buildRequest(group[0].cmd, group[0].key, raw)
	resp := buildQuietBarrierResponse()
	defer releaseReqAndResp(req, resp)

	var parsed []*MetaItem
	gotLines := false
	resp.afterRecv = func(r *response) error {
		items, err := parseMetaPipeline(r.rawLines)
		parsed = items
		gotLines = true
		return err
	}
	dispatchErr := c.dispatchToAddr(ctx, node, req, resp)
	if !gotLines {
		return dispatchErr
	}
	for _, p := range group {
		if p.noop {
			out[p.idx] = MetaResult{Status: MetaStatusMN, Item: MetaItem{Key: p.key}}
		}
	}
	for _, item := range parsed {
		idx, ok := byOpaque[item.Opaque]
		if !ok {
			continue
		}
		if len(item.Key) == 0 {
			item.Key = out[idx].Item.Key
		}
		for _, p := range group {
			if p.idx == idx && p.getFlags != nil {
				applyCheckCAS(item, p.getFlags)
				break
			}
		}
		out[idx] = metaResultFrom(item, item.status)
	}
	for _, p := range group {
		if out[p.idx].Status != MetaStatusUnknown {
			continue
		}
		if isChargeMetaFault(dispatchErr) {
			out[p.idx] = metaResultWithErr(&MetaItem{Key: p.key}, MetaStatusUnknown, dispatchErr)
			continue
		}
		switch {
		case p.noop:
			out[p.idx] = MetaResult{Status: MetaStatusMN, Item: MetaItem{Key: p.key}}
		case string(p.cmd) == "mg":
			out[p.idx] = MetaResult{Status: MetaStatusEN, Item: MetaItem{Key: p.key}}
		default:
			out[p.idx] = MetaResult{Status: MetaStatusHD, Item: MetaItem{Key: p.key}}
		}
	}
	return dispatchErr
}
