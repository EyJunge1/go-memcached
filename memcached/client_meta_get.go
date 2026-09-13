package memcached

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
)

func stripTrailingMN(raw []byte) []byte {
	return bytes.TrimSuffix(raw, []byte("mn\r\n"))
}

// MetaGet fetches a key with meta flags.
func (c *Client) MetaGet(ctx context.Context, key []byte, f MetaGet) (MetaResult, error) {
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
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return metaResultWithErr(item, item.Status, err), fmt.Errorf("request failed: %w", err)
	}
	applyCheckCAS(item, mgFlags)
	return metaResultFrom(item, item.Status), nil
}

func applyCheckCAS(item *MetaItem, flags *metaGetFlags) {
	if item == nil || flags == nil {
		return
	}
	item.CASMatched = flags.Cset && flags.v && item.gotHD && item.Value == nil
}

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

func metaOpFlags(op MetaOp) (any, error) {
	if op.Flags == nil {
		switch op.Op {
		case MetaOpGet:
			return MetaGet{}, nil
		case MetaOpSet:
			return MetaSet{}, nil
		case MetaOpDelete:
			return MetaDelete{}, nil
		case MetaOpArith:
			return MetaArithmetic{}, nil
		case MetaOpNoop:
			return MetaNoop{}, nil
		default:
			return nil, fmt.Errorf("meta op: %w", ErrInvalidArgument)
		}
	}
	return op.Flags, nil
}

func encodeMetaPipelineOp(c *Client, op MetaOp, idx int) (preparedMetaOp, error) {
	key := c.nsBytes(op.Key)
	flagsAny, err := metaOpFlags(op)
	if err != nil {
		return preparedMetaOp{}, err
	}
	prep := preparedMetaOp{idx: idx, key: key}

	switch op.Op {
	case MetaOpGet:
		f, ok := flagsAny.(MetaGet)
		if !ok {
			return preparedMetaOp{}, fmt.Errorf("meta get flags: %w", ErrInvalidArgument)
		}
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
	case MetaOpSet:
		f, ok := flagsAny.(MetaSet)
		if !ok {
			return preparedMetaOp{}, fmt.Errorf("meta set flags: %w", ErrInvalidArgument)
		}
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
	case MetaOpDelete:
		f, ok := flagsAny.(MetaDelete)
		if !ok {
			return preparedMetaOp{}, fmt.Errorf("meta delete flags: %w", ErrInvalidArgument)
		}
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
	case MetaOpArith:
		f, ok := flagsAny.(MetaArithmetic)
		if !ok {
			return preparedMetaOp{}, fmt.Errorf("meta arith flags: %w", ErrInvalidArgument)
		}
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
	case MetaOpNoop:
		// Trailing mn is the pipeline barrier; an extra mn here would end read2 early.
		f, ok := flagsAny.(MetaNoop)
		if flagsAny != nil && !ok {
			return preparedMetaOp{}, fmt.Errorf("meta noop flags: %w", ErrInvalidArgument)
		}
		if err := f.validate(); err != nil {
			return preparedMetaOp{}, err
		}
		prep.cmd = []byte("mn")
		prep.line = nil
		prep.noop = true
		prep.barrier = f
	default:
		return preparedMetaOp{}, fmt.Errorf("meta op: %w", ErrInvalidArgument)
	}
	return prep, nil
}

func opaqueToken(tok string, num uint64, set bool) string {
	if tok != "" {
		return tok
	}
	if set {
		return strconv.FormatUint(num, 10)
	}
	return ""
}

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

// MetaPipeline issues mixed meta ops with auto-O, a trailing mn, and parallel fan-out.
func (c *Client) MetaPipeline(ctx context.Context, ops []MetaOp) ([]MetaResult, error) {
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

	groups := make(map[*Addr][]preparedMetaOp, len(addrs))
	for _, p := range prepared {
		addr, err := picker.Pick(addrs, c.hashKey(p.key))
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

func (c *Client) metaPipelineDispatch(ctx context.Context, addr *Addr, group []preparedMetaOp, out []MetaResult) error {
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
	dispatchErr := c.dispatchToAddr(ctx, addr, req, resp)
	if !gotLines {
		return dispatchErr
	}
	for _, p := range group {
		if p.noop {
			out[p.idx] = MetaResult{Status: MetaStatusMN, Item: MetaItem{Key: p.key, Status: MetaStatusMN}}
		}
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
			item.Key = out[idx].Item.Key
		}
		for _, p := range group {
			if p.idx == idx && p.getFlags != nil {
				applyCheckCAS(item, p.getFlags)
				break
			}
		}
		out[idx] = metaResultFrom(item, item.Status)
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
			out[p.idx] = MetaResult{Status: MetaStatusMN, Item: MetaItem{Key: p.key, Status: MetaStatusMN}}
		case string(p.cmd) == "mg":
			out[p.idx] = MetaResult{Status: MetaStatusEN, Item: MetaItem{Key: p.key, Status: MetaStatusEN}}
		default:
			out[p.idx] = MetaResult{Status: MetaStatusHD, Item: MetaItem{Key: p.key, Status: MetaStatusHD}}
		}
	}
	return dispatchErr
}
