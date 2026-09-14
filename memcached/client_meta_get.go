package memcached

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
)

// Get sends mg for key using flags f and writes the outcome into dst.
// dst must be non-nil. If dst.Item.Value has enough cap, a VA body is written
// there. Key is copied only when f.Return.Key is set.
func (m *MetaClient) Get(ctx context.Context, key []byte, f MetaGet, dst *MetaResult) error {
	if dst == nil {
		return fmt.Errorf("meta get dst: %w", ErrInvalidArgument)
	}
	c := m.c
	if err := f.validate(); err != nil {
		return err
	}
	mgFlags := f.toFlags()
	key = c.nsBytes(key)
	if err := c.checkKeyValueFlags(key, nil, mgFlags.b); err != nil {
		return err
	}
	if mgFlags.OtokenSet {
		if err := validOpaqueToken(mgFlags.Otoken); err != nil {
			return err
		}
	}

	keep := dst.Item.Value
	*dst = MetaResult{}
	item := MetaItem{Value: keep}
	req, resp := buildMetaGetCommand(key, &mgFlags)
	resp.metaItem = &item
	defer releaseReqAndResp(req, resp)

	dispatchErr := c.dispatchRequest(ctx, req, resp)
	parseErr := error(nil)
	if resp.metaFilled {
		parseErr = metaItemStatusErr(&item, mgFlags.q, true)
	} else {
		parseErr = parseMetaItem(resp.rawLines, &item, mgFlags.q, true)
	}
	res, err := finishMetaParse(&item, dispatchErr, parseErr)
	if err != nil {
		*dst = MetaResult{}
		dst.Item.Value = keep
		return err
	}
	applyCheckCAS(&item, &mgFlags)
	res.Item.CASMatched = item.CASMatched
	if !f.Return.Key {
		res.Item.Key = nil
		res.Item.WireKey = nil
	} else if len(res.Item.Key) == 0 {
		res.Item.Key = append([]byte(nil), c.stripNS(key)...)
	} else if c.hasNS() {
		res.Item.Key = c.stripNS(res.Item.Key)
	}
	*dst = res
	return nil
}

func metaItemStatusErr(item *MetaItem, noReply, quietMissAsNotFound bool) error {
	if item == nil {
		return fmt.Errorf("missing response: %w", ErrMalformedResponse)
	}
	switch item.status {
	case MetaStatusNF, MetaStatusEN:
		return ErrNotFound
	case MetaStatusNS:
		return ErrNotStored
	case MetaStatusEX:
		return ErrExists
	case MetaStatusHD:
		item.gotHD = true
		return nil
	case MetaStatusVA, MetaStatusMN, MetaStatusME:
		return nil
	case MetaStatusUnknown:
		if noReply && len(item.Value) == 0 && !item.gotHD {
			if quietMissAsNotFound {
				item.status = MetaStatusEN
				return ErrNotFound
			}
			item.status = MetaStatusHD
			item.gotHD = true
			return nil
		}
		return fmt.Errorf("missing response: %w", ErrMalformedResponse)
	default:
		return nil
	}
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
	idx       int
	key       []byte
	cmd       []byte
	line      []byte
	body      []byte
	tail      []byte
	lineBox   *[]byte
	tok       string
	tokNum    uint64
	hasTok    bool
	getFlags  metaGetFlags
	hasGet    bool
	returnKey bool
	noop      bool
	barrier   MetaNoop
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
		if err := ensurePipelineOpaque(&mg, idx); err != nil {
			return preparedMetaOp{}, err
		}
		_, line, box := encodeMetaGetOwned(key, &mg)
		prep.cmd = _MgBytes
		prep.line = line
		prep.lineBox = box
		prep.tok = opaqueToken(mg.Otoken, mg.O, mg.OtokenSet)
		prep.tokNum = mg.O
		prep.hasTok = mg.Oset || mg.OtokenSet
		prep.getFlags = mg
		prep.hasGet = true
		prep.returnKey = f.Return.Key
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
		if err := ensureSetOpaque(&ms, idx); err != nil {
			return preparedMetaOp{}, err
		}
		header, box, body, tail := encodeMetaSetOwned(key, op.Body, &ms)
		prep.cmd = _MsBytes
		prep.line = header
		prep.lineBox = box
		prep.body = body
		prep.tail = tail
		prep.tok = opaqueToken(ms.Otoken, ms.O, ms.OtokenSet)
		prep.tokNum = ms.O
		prep.hasTok = ms.Oset || ms.OtokenSet
		prep.key = key
		prep.returnKey = f.ReturnKey
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
		if err := ensureDeleteOpaque(&md, idx); err != nil {
			return preparedMetaOp{}, err
		}
		line, box := encodeMetaDeleteOwned(key, &md)
		prep.cmd = _MdBytes
		prep.line = line
		prep.lineBox = box
		prep.tok = opaqueToken(md.Otoken, md.O, md.OtokenSet)
		prep.tokNum = md.O
		prep.hasTok = md.Oset || md.OtokenSet
		prep.returnKey = f.ReturnKey
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
		if err := ensureArithOpaque(&ma, idx); err != nil {
			return preparedMetaOp{}, err
		}
		line, box := encodeMetaArithmeticOwned(key, op.Delta, &ma)
		prep.cmd = _MaBytes
		prep.line = line
		prep.lineBox = box
		prep.tok = opaqueToken(ma.Otoken, ma.O, ma.OtokenSet)
		prep.tokNum = ma.O
		prep.hasTok = ma.Oset || ma.OtokenSet
		prep.returnKey = f.ReturnKey
	case op.Noop != nil:
		// The batch already ends with mn; another mn would make the barrier reader stop early.
		f := *op.Noop
		if err := f.validate(); err != nil {
			return preparedMetaOp{}, err
		}
		prep.cmd = _MnBytes
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
	flags.O = uint64(idx) //nolint:gosec // G115: pipeline index
	flags.Oset = true
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
	flags.O = uint64(idx) //nolint:gosec // G115: pipeline index
	flags.Oset = true
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
	flags.O = uint64(idx) //nolint:gosec // G115: pipeline index
	flags.Oset = true
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
	flags.O = uint64(idx) //nolint:gosec // G115: pipeline index
	flags.Oset = true
	return nil
}

// Pipeline runs mixed meta ops, assigns O when missing, appends mn, and fans out by node.
// len(dst) must equal len(ops). Each dst[i] is filled for ops[i]. Value buffers
// with enough cap are reused.
func (m *MetaClient) Pipeline(ctx context.Context, ops []MetaOp, dst []MetaResult) error {
	c := m.c
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(ops) != len(dst) {
		return fmt.Errorf("meta pipeline dst length %d != ops %d: %w", len(dst), len(ops), ErrInvalidArgument)
	}
	if len(ops) == 0 {
		return nil
	}
	for i := range dst {
		keep := dst[i].Item.Value
		dst[i] = MetaResult{}
		if keep != nil {
			dst[i].Item.Value = keep[:0]
		}
	}

	prepared := make([]preparedMetaOp, 0, len(ops))
	out := dst
	for i, op := range ops {
		p, err := encodeMetaPipelineOp(c, op, i)
		if err != nil {
			for _, prev := range prepared {
				putBuilderBox(prev.lineBox)
			}
			return err
		}
		prepared = append(prepared, p)
		if p.returnKey {
			out[i].Item.Key = p.key
		}
	}

	addrs, picker := c.snapshotCluster()
	if picker == nil || len(addrs) == 0 {
		return fmt.Errorf("no servers configured: %w", ErrInvalidAddress)
	}
	addrs = c.liveAddrs(addrs)
	if len(addrs) == 1 {
		return c.metaPipelineDispatch(ctx, addrs[0], prepared, out)
	}

	groups := make(map[*addr][]preparedMetaOp, len(addrs))
	for _, p := range prepared {
		addr, err := picker.Pick(addrs, p.key)
		if err != nil {
			return fmt.Errorf("pick node failed: %w", err)
		}
		groups[addr] = append(groups[addr], p)
	}

	if len(groups) == 1 {
		for addr, group := range groups {
			return c.metaPipelineDispatch(ctx, addr, group, out)
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
	return errors.Join(errs...)
}

// metaPipelineDispatch writes a node's quiet meta batch, then maps replies onto out by opaque.
func (c *Client) metaPipelineDispatch(ctx context.Context, node *addr, group []preparedMetaOp, out []MetaResult) error {
	var raw []byte
	var barrier MetaNoop
	var byOpaque map[string]int
	for i := range group {
		p := &group[i]
		raw = append(raw, p.line...)
		raw = append(raw, p.body...)
		raw = append(raw, p.tail...)
		putBuilderBox(p.lineBox)
		p.lineBox = nil
		if p.noop {
			barrier = p.barrier
		}
		if p.tok == "" {
			continue
		}
		if byOpaque == nil {
			byOpaque = make(map[string]int, len(group))
		}
		if _, exists := byOpaque[p.tok]; exists {
			return fmt.Errorf("duplicate opaque %q: %w", p.tok, ErrInvalidArgument)
		}
		byOpaque[p.tok] = p.idx
	}
	raw = appendMetaQuietBarrier(raw, barrier)

	req := buildRequest(group[0].cmd, group[0].key, raw)
	resp := buildQuietBarrierResponse()
	resp.metaPipeC = c
	resp.metaPipeDst = out
	resp.metaPipeGroup = group
	resp.metaPipeByOpaque = byOpaque
	defer releaseReqAndResp(req, resp)

	dispatchErr := c.dispatchToAddr(ctx, node, req, resp)
	if dispatchErr != nil && !resp.metaSawReply && !isChargeMetaFault(dispatchErr) {
		return dispatchErr
	}
	for _, p := range group {
		if p.noop {
			storeMetaPipelineResult(c, out, p.idx, &p, MetaResult{Status: MetaStatusMN})
		}
	}
	for _, p := range group {
		if out[p.idx].Status != MetaStatusUnknown {
			continue
		}
		if isChargeMetaFault(dispatchErr) {
			storeMetaPipelineResult(c, out, p.idx, &p, metaResultWithErr(nil, MetaStatusUnknown, dispatchErr))
			continue
		}
		switch {
		case p.noop:
			storeMetaPipelineResult(c, out, p.idx, &p, MetaResult{Status: MetaStatusMN})
		case bytes.Equal(p.cmd, _MgBytes):
			storeMetaPipelineResult(c, out, p.idx, &p, MetaResult{Status: MetaStatusEN})
		default:
			storeMetaPipelineResult(c, out, p.idx, &p, MetaResult{Status: MetaStatusHD})
		}
	}
	return dispatchErr
}

func (resp *response) fillMetaPipe(rr protocolReader, header []byte) error {
	if resp == nil || resp.metaPipeDst == nil {
		return fmt.Errorf("missing meta pipeline dest: %w", ErrMalformedResponse)
	}
	resp.metaSawReply = true
	var item MetaItem
	_, size, hasSize, err := parseMetaHeader(header, &item)
	if err != nil {
		return err
	}
	if item.status == MetaStatusHD {
		item.gotHD = true
	}
	idx, ok := matchMetaPipelineOp(&item, resp.metaPipeGroup, resp.metaPipeByOpaque)
	if item.status == MetaStatusVA {
		if !hasSize {
			return fmt.Errorf("missing value size: %w", ErrMalformedResponse)
		}
		limit := resp.maxValueBytes
		if limit <= 0 {
			limit = defaultMaxValueBytes
		}
		if size > uint64(limit) {
			return fmt.Errorf("data block too large: %w", ErrMalformedResponse)
		}
		n := int(size) //nolint:gosec // G115: size is bounded above
		if ok {
			keep := resp.metaPipeDst[idx].Item.Value
			buf := takeValueBuffer(keep, n)
			if err = readFramedValueInto(rr, buf); err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
					return fmt.Errorf("short data block: %w", ErrMalformedResponse)
				}
				return fmt.Errorf("read data block: %w", err)
			}
			item.Value = buf
		} else if err = discardFramedValue(rr, n); err != nil {
			return err
		}
	}
	if !ok {
		return nil
	}
	var prep *preparedMetaOp
	for i := range resp.metaPipeGroup {
		if resp.metaPipeGroup[i].idx == idx {
			prep = &resp.metaPipeGroup[i]
			if prep.hasGet {
				applyCheckCAS(&item, &prep.getFlags)
			}
			break
		}
	}
	storeMetaPipelineResult(resp.metaPipeC, resp.metaPipeDst, idx, prep, metaResultFrom(&item, item.status))
	return nil
}

func storeMetaPipelineResult(c *Client, out []MetaResult, idx int, p *preparedMetaOp, res MetaResult) {
	keep := out[idx].Item.Value
	if p == nil || !p.returnKey {
		res.Item.Key = nil
		res.Item.WireKey = nil
	} else if len(res.Item.Key) == 0 {
		res.Item.Key = append([]byte(nil), c.stripNS(p.key)...)
	} else if c.hasNS() {
		res.Item.Key = c.stripNS(res.Item.Key)
	}
	res.Item.Value = ownValue(keep, res.Item.Value)
	out[idx] = res
}

func matchMetaPipelineOp(item *MetaItem, group []preparedMetaOp, byOpaque map[string]int) (int, bool) {
	if item.hasOpaqueNum {
		for _, p := range group {
			if p.tok == "" && p.hasTok && p.tokNum == item.opaqueNum {
				return p.idx, true
			}
		}
	}
	if item.Opaque != "" {
		if idx, ok := byOpaque[item.Opaque]; ok {
			return idx, true
		}
	}
	return 0, false
}
