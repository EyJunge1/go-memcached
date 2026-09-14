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
// Transport and parse failures are the method error. EN, NF, NS, and EX live
// on dst.Status / dst.Err(); the method error stays nil.
func (m *MetaClient) Get(ctx context.Context, key []byte, dst *MetaResult, f MetaGet) error {
	if dst == nil {
		return fmt.Errorf("meta get dst: %w", ErrInvalidArgument)
	}
	c := m.c
	if err := f.validate(); err != nil {
		return err
	}
	mgFlags := f.toFlags()
	key, keyBox := c.nsKey(key)
	if err := c.checkKeyValueFlags(key, nil, mgFlags.b); err != nil {
		putBuilderBox(keyBox)
		return err
	}
	if mgFlags.OtokenSet {
		if err := validOpaqueToken(mgFlags.Otoken); err != nil {
			putBuilderBox(keyBox)
			return err
		}
	}

	keep := takeMetaKeep(dst)
	req, resp := buildMetaGetCommand(key, &mgFlags)
	req.adoptKeyBox(keyBox)
	resp.metaItem = &dst.Item
	defer releaseReqAndResp(req, resp)

	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		resetMetaGetDest(dst, keep)
		return fmt.Errorf("request failed: %w", err)
	}
	if err := resp.metaStatusErr(mgFlags.q, true); err != nil {
		if cd := dst.Item.status.Err(); cd == nil || !errors.Is(err, cd) {
			resetMetaGetDest(dst, keep)
			return fmt.Errorf("request failed: %w", err)
		}
	}
	applyMetaGetDest(dst, keep, c, key, f, &mgFlags)
	return nil
}

func resetMetaGetDest(dst *MetaResult, keep metaKeep) {
	*dst = MetaResult{}
	restoreMetaKeep(dst, keep)
}

func applyMetaGetDest(dst *MetaResult, keep metaKeep, c *Client, key []byte, f MetaGet, flags *metaGetFlags) {
	item := &dst.Item
	applyCheckCAS(item, flags)
	if item.status != MetaStatusVA {
		if keep.value != nil {
			item.Value = keep.value[:0]
		} else {
			item.Value = nil
		}
	}
	dst.Status = item.status
	item.status = 0
	if !f.Return.Key {
		item.Key = nil
		if keep.key != nil {
			item.WireKey = keep.key[:0]
		} else {
			item.WireKey = nil
		}
		return
	}
	copyMetaUserKey(item, keep.userKey, c, c.stripNS(key), true)
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

// applyCheckCAS sets CASMatched when mg used C+v and the server returned HD.
// A reused dest buffer must not hide the match: HD has no VA body.
func applyCheckCAS(item *MetaItem, flags *metaGetFlags) {
	if item == nil || flags == nil {
		return
	}
	item.CASMatched = flags.Cset && flags.v && item.gotHD && item.status != MetaStatusVA
}

// preparedMetaOp is one encoded Meta().Pipeline step plus its opaque token.
type preparedMetaOp struct {
	idx         int
	key         []byte
	nodeIndex   int
	cmd         []byte
	line        []byte
	body        []byte
	tail        []byte
	lineBox     *[]byte
	keyBox      *[]byte
	tok         string
	tokNum      uint64
	hasTok      bool
	getFlags    metaGetFlags
	hasGet      bool
	returnKey   bool
	valueBinary bool
	keepUser    []byte
	keepOpaque  []byte
	noop        bool
	barrier     MetaNoop
}

const maxPooledPreparedMeta = 256

var preparedMetaPool = sync.Pool{
	New: func() any {
		s := make([]preparedMetaOp, 0, 16)
		return &s
	},
}

const (
	defaultMetaBatchBuffer = 8 << 10
	maxMetaBatchBuffer     = 64 << 10
)

var metaBatchBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, defaultMetaBatchBuffer)
		return &buf
	},
}

func takeMetaBatchBuffer() *[]byte {
	box := metaBatchBufferPool.Get().(*[]byte)
	*box = (*box)[:0]
	return box
}

func putMetaBatchBuffer(box *[]byte) {
	if box == nil || cap(*box) > maxMetaBatchBuffer {
		return
	}
	*box = (*box)[:0]
	metaBatchBufferPool.Put(box)
}

func getPreparedMeta(n int) *[]preparedMetaOp {
	p := preparedMetaPool.Get().(*[]preparedMetaOp)
	if cap(*p) < n {
		s := make([]preparedMetaOp, 0, n)
		*p = s
		return p
	}
	*p = (*p)[:0]
	return p
}

func putPreparedMeta(p *[]preparedMetaOp) {
	if p == nil {
		return
	}
	s := *p
	for i := range s {
		putBuilderBox(s[i].lineBox)
		putBuilderBox(s[i].keyBox)
		s[i] = preparedMetaOp{}
	}
	if cap(s) > maxPooledPreparedMeta {
		return
	}
	*p = s[:0]
	preparedMetaPool.Put(p)
}

// metaOpKinds counts how many of Get, Set, Delete, Arithmetic, and Noop are set on op.
func metaOpKinds(op MetaOp) int {
	n := 0
	if op.Get != nil {
		n++
	}
	if op.Set != nil {
		n++
	}
	if op.Delete != nil {
		n++
	}
	if op.Arithmetic != nil {
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
	if idx < 0 {
		return preparedMetaOp{}, fmt.Errorf("pipeline index: %w", ErrInvalidArgument)
	}
	opaque := uint64(idx)
	key, keyBox := c.nsKey(op.Key)
	prep := preparedMetaOp{idx: idx, key: key, keyBox: keyBox}

	switch {
	case op.Get != nil:
		f := *op.Get
		if err := f.validate(); err != nil {
			putBuilderBox(keyBox)
			return preparedMetaOp{}, err
		}
		mg := f.toFlags()
		if !mg.Cset {
			mg.q = true
		}
		if err := c.checkKeyValueFlags(key, nil, mg.b); err != nil {
			putBuilderBox(keyBox)
			return preparedMetaOp{}, err
		}
		if err := ensurePipelineOpaque(&mg, opaque); err != nil {
			putBuilderBox(keyBox)
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
			putBuilderBox(keyBox)
			return preparedMetaOp{}, err
		}
		if err := f.validate(); err != nil {
			putBuilderBox(keyBox)
			return preparedMetaOp{}, err
		}
		if !validMetaSetMode(ms.M) {
			putBuilderBox(keyBox)
			return preparedMetaOp{}, fmt.Errorf("invalid meta set mode: %w", ErrInvalidArgument)
		}
		if err := ensureSetOpaque(&ms, opaque); err != nil {
			putBuilderBox(keyBox)
			return preparedMetaOp{}, err
		}
		header, box, body, tail := encodeMetaSetOwned(key, op.Body, &ms)
		prep.cmd = _MsBytes
		prep.line = header
		prep.lineBox = box
		prep.body = body
		prep.tail = tail
		prep.valueBinary = ms.b
		prep.tok = opaqueToken(ms.Otoken, ms.O, ms.OtokenSet)
		prep.tokNum = ms.O
		prep.hasTok = ms.Oset || ms.OtokenSet
		prep.key = key
		prep.returnKey = f.Return.Key
	case op.Delete != nil:
		f := *op.Delete
		if err := f.validate(); err != nil {
			putBuilderBox(keyBox)
			return preparedMetaOp{}, err
		}
		md := f.toFlags()
		md.q = true
		if err := c.checkKeyValueFlags(key, nil, md.b); err != nil {
			putBuilderBox(keyBox)
			return preparedMetaOp{}, err
		}
		if err := ensureDeleteOpaque(&md, opaque); err != nil {
			putBuilderBox(keyBox)
			return preparedMetaOp{}, err
		}
		line, box := encodeMetaDeleteOwned(key, &md)
		prep.cmd = _MdBytes
		prep.line = line
		prep.lineBox = box
		prep.tok = opaqueToken(md.Otoken, md.O, md.OtokenSet)
		prep.tokNum = md.O
		prep.hasTok = md.Oset || md.OtokenSet
		prep.returnKey = f.Return.Key
	case op.Arithmetic != nil:
		f := *op.Arithmetic
		if err := f.validate(); err != nil {
			putBuilderBox(keyBox)
			return preparedMetaOp{}, err
		}
		ma := f.toFlags()
		if !ma.v {
			ma.q = true
		}
		if err := c.checkKeyValueFlags(key, nil, ma.b); err != nil {
			putBuilderBox(keyBox)
			return preparedMetaOp{}, err
		}
		if !validMetaArithmeticMode(ma.M) {
			putBuilderBox(keyBox)
			return preparedMetaOp{}, fmt.Errorf("invalid meta arithmetic mode: %w", ErrInvalidArgument)
		}
		if err := ensureArithOpaque(&ma, opaque); err != nil {
			putBuilderBox(keyBox)
			return preparedMetaOp{}, err
		}
		line, box := encodeMetaArithmeticOwned(key, op.Delta, &ma)
		prep.cmd = _MaBytes
		prep.line = line
		prep.lineBox = box
		prep.tok = opaqueToken(ma.Otoken, ma.O, ma.OtokenSet)
		prep.tokNum = ma.O
		prep.hasTok = ma.Oset || ma.OtokenSet
		prep.returnKey = f.Return.Key
	case op.Noop != nil:
		// The batch already ends with mn; another mn would make the barrier reader stop early.
		f := *op.Noop
		if err := f.validate(); err != nil {
			putBuilderBox(keyBox)
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
func ensurePipelineOpaque(flags *metaGetFlags, idx uint64) error {
	if flags.OtokenSet {
		return validOpaqueToken(flags.Otoken)
	}
	if flags.Oset {
		return nil
	}
	flags.O = idx
	flags.Oset = true
	return nil
}

// ensureSetOpaque assigns a unique O token for a pipelined ms when the caller omitted one.
func ensureSetOpaque(flags *metaSetFlags, idx uint64) error {
	if flags.OtokenSet {
		return validOpaqueToken(flags.Otoken)
	}
	if flags.Oset {
		return nil
	}
	flags.O = idx
	flags.Oset = true
	return nil
}

// ensureDeleteOpaque assigns a unique O token for a pipelined md when the caller omitted one.
func ensureDeleteOpaque(flags *metaDeleteFlags, idx uint64) error {
	if flags.OtokenSet {
		return validOpaqueToken(flags.Otoken)
	}
	if flags.Oset {
		return nil
	}
	flags.O = idx
	flags.Oset = true
	return nil
}

// ensureArithOpaque assigns a unique O token for a pipelined ma when the caller omitted one.
func ensureArithOpaque(flags *metaArithmeticFlags, idx uint64) error {
	if flags.OtokenSet {
		return validOpaqueToken(flags.Otoken)
	}
	if flags.Oset {
		return nil
	}
	flags.O = idx
	flags.Oset = true
	return nil
}

// Pipeline runs mixed meta ops, assigns O when missing, appends mn, and fans out by node.
// len(dst) must equal len(ops). Each dst[i] is filled for ops[i]. Value buffers
// with enough cap are reused.
// Transport and parse failures are the method error. EN, NF, NS, and EX live
// on dst[i].Status / dst[i].Err(); the method error stays nil.
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
		_ = takeMetaKeep(&dst[i])
	}

	prepBox := getPreparedMeta(len(ops))
	defer putPreparedMeta(prepBox)
	out := dst
	for i, op := range ops {
		p, err := encodeMetaPipelineOp(c, op, i)
		if err != nil {
			return err
		}
		p.keepUser = out[i].Item.Key
		p.keepOpaque = out[i].Item.Opaque
		*prepBox = append(*prepBox, p)
	}
	prepared := *prepBox

	addrs, picker := c.snapshotCluster()
	if picker == nil || len(addrs) == 0 {
		return fmt.Errorf("no servers configured: %w", ErrInvalidAddress)
	}
	addrs = c.liveAddrs(addrs)
	if len(addrs) == 1 {
		return c.metaPipelineDispatch(ctx, addrs[0], prepared, out)
	}

	var first *addr
	sameNode := true
	for i := range prepared {
		nodeIndex, addr, err := pickAddrIndex(picker, addrs, prepared[i].key)
		if err != nil {
			return fmt.Errorf("pick node failed: %w", err)
		}
		prepared[i].nodeIndex = nodeIndex
		if first == nil {
			first = addr
		} else if addr != first {
			sameNode = false
		}
	}
	if sameNode {
		return c.metaPipelineDispatch(ctx, first, prepared, out)
	}

	var countStack [32]int
	var offsetStack [33]int
	var counts, offsets []int
	if len(addrs) <= len(countStack) {
		counts = countStack[:len(addrs)]
		offsets = offsetStack[:len(addrs)+1]
	} else {
		counts = make([]int, len(addrs))
		offsets = make([]int, len(addrs)+1)
	}
	for i := range prepared {
		counts[prepared[i].nodeIndex]++
	}
	for i := range addrs {
		offsets[i+1] = offsets[i] + counts[i]
		counts[i] = offsets[i]
	}
	groupBox := getPreparedMeta(len(prepared))
	if cap(*groupBox) < len(prepared) {
		*groupBox = make([]preparedMetaOp, len(prepared))
	} else {
		*groupBox = (*groupBox)[:len(prepared)]
	}
	defer putPreparedMeta(groupBox)
	grouped := *groupBox
	for i := range prepared {
		group := prepared[i].nodeIndex
		copyOp := prepared[i]
		copyOp.keyBox = nil
		grouped[counts[group]] = copyOp
		counts[group]++
		prepared[i].lineBox = nil
	}

	var (
		mu   sync.Mutex
		errs []error
	)
	active := 0
	for i := range addrs {
		if offsets[i] != offsets[i+1] {
			counts[active] = i
			active++
		}
	}
	c.runFanout(ctx, active, func(runCtx context.Context, groupIndex int) {
		i := counts[groupIndex]
		if err := c.metaPipelineDispatch(runCtx, addrs[i], grouped[offsets[i]:offsets[i+1]], out); err != nil {
			mu.Lock()
			errs = append(errs, err)
			mu.Unlock()
		}
	})
	return errors.Join(errs...)
}

// metaPipelineDispatch writes a node's quiet meta batch, then maps replies onto out by opaque.
func (c *Client) metaPipelineDispatch(ctx context.Context, node *addr, group []preparedMetaOp, out []MetaResult) error {
	if len(group) == 0 {
		return nil
	}
	box := takeMetaBatchBuffer()
	index, err := buildMetaPipelineIndex(group)
	if err != nil {
		putMetaBatchBuffer(box)
		return err
	}
	var barrier MetaNoop
	need := 0
	for i := range group {
		p := &group[i]
		need += len(p.line) + len(p.body) + len(p.tail)
		if p.noop {
			barrier = p.barrier
		}
	}
	need += metaQuietBarrierLen(barrier)
	if cap(*box) < need {
		*box = make([]byte, 0, need)
	}
	raw := (*box)[:0]
	for i := range group {
		p := &group[i]
		raw = append(raw, p.line...)
		raw = append(raw, p.body...)
		raw = append(raw, p.tail...)
	}
	raw = appendMetaQuietBarrier(raw, barrier)
	*box = raw

	req := buildRequest(group[0].cmd, group[0].key, raw)
	req.rawBox = box
	req.rawPut = putMetaBatchBuffer
	resp := buildQuietBarrierResponse()
	resp.metaPipeC = c
	resp.metaPipeDst = out
	resp.metaPipeGroup = group
	resp.metaPipeIndex = index
	resetMetaPipeSeen(resp, len(group))
	defer releaseReqAndResp(req, resp)

	return c.finishMetaPipelineDispatch(ctx, node, group, out, req, resp)
}

func (c *Client) finishMetaPipelineDispatch(
	ctx context.Context,
	node *addr,
	group []preparedMetaOp,
	out []MetaResult,
	req *request,
	resp *response,
) error {
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

func metaQuietBarrierLen(f MetaNoop) int {
	n := len(_MnCRLFBytes)
	if f.ProxyHint != "" {
		n += 2 + len(f.ProxyHint)
	}
	if f.ProxyPath != "" {
		n += 2 + len(f.ProxyPath)
	}
	return n
}

func (resp *response) fillMetaPipe(rr protocolReader, header []byte) error {
	if resp == nil || resp.metaPipeDst == nil {
		return fmt.Errorf("missing meta pipeline dest: %w", ErrMalformedResponse)
	}
	resp.metaSawReply = true
	pos, ok := matchMetaPipelineOpaque(header, resp.metaPipeGroup, resp.metaPipeIndex)
	idx := 0
	var prep *preparedMetaOp
	if ok {
		prep = &resp.metaPipeGroup[pos]
		idx = prep.idx
		if metaPipeOpaqueSeen(resp, pos) {
			return fmt.Errorf("duplicate meta opaque response: %w", ErrMalformedResponse)
		}
		markMetaPipeOpaqueSeen(resp, pos)
	}

	var item *MetaItem
	var scratch MetaItem
	if ok {
		item = &resp.metaPipeDst[idx].Item
	} else {
		item = &scratch
	}
	_, size, hasSize, err := parseMetaHeader(header, item)
	if err != nil {
		return err
	}
	if item.status == MetaStatusHD {
		item.gotHD = true
	}
	if item.status == MetaStatusVA {
		if !hasSize {
			return fmt.Errorf("missing value size: %w", ErrMalformedResponse)
		}
		n, err := cappedValueSize(size, resp.maxValueBytes)
		if err != nil {
			return err
		}
		if ok {
			item.Value = takeValueBuffer(item.Value, n)
			if err = readFramedValueInto(rr, item.Value); err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
					return fmt.Errorf("short data block: %w", ErrMalformedResponse)
				}
				return fmt.Errorf("read data block: %w", err)
			}
		} else if err = discardFramedValue(rr, n); err != nil {
			return err
		}
	}
	if !ok {
		return nil
	}
	if prep.hasGet {
		applyCheckCAS(item, &prep.getFlags)
	}
	finishMetaPipelineDest(resp.metaPipeC, resp.metaPipeDst, idx, prep)
	return nil
}

func metaPipeOpaqueSeen(resp *response, pos int) bool {
	byteIdx := pos >> 3
	if byteIdx >= len(resp.metaPipeSeen) {
		return false
	}
	return resp.metaPipeSeen[byteIdx]&(1<<uint(pos&7)) != 0
}

func markMetaPipeOpaqueSeen(resp *response, pos int) {
	need := (pos >> 3) + 1
	if len(resp.metaPipeSeen) < need {
		grown := make([]byte, need)
		copy(grown, resp.metaPipeSeen)
		resp.metaPipeSeen = grown
	}
	resp.metaPipeSeen[pos>>3] |= 1 << uint(pos&7)
}

func resetMetaPipeSeen(resp *response, n int) {
	if resp == nil {
		return
	}
	need := (n + 7) >> 3
	if cap(resp.metaPipeSeen) >= need {
		resp.metaPipeSeen = resp.metaPipeSeen[:need]
		clear(resp.metaPipeSeen)
		return
	}
	resp.metaPipeSeen = make([]byte, need)
}

func finishMetaPipelineDest(c *Client, out []MetaResult, idx int, p *preparedMetaOp) {
	dst := &out[idx]
	dst.Status = dst.Item.status
	dst.Item.status = 0
	if p == nil || !p.returnKey {
		dst.Item.Key = nil
		if dst.Item.WireKey != nil {
			dst.Item.WireKey = dst.Item.WireKey[:0]
		}
		return
	}
	fallback := []byte(nil)
	if c != nil {
		fallback = c.stripNS(p.key)
	}
	copyMetaUserKey(&dst.Item, p.keepUser, c, fallback, true)
}

func storeMetaPipelineResult(c *Client, out []MetaResult, idx int, p *preparedMetaOp, res MetaResult) {
	keepVal := out[idx].Item.Value
	keepWire := out[idx].Item.WireKey
	keepUser := []byte(nil)
	keepOpaque := []byte(nil)
	if p != nil {
		keepUser = p.keepUser
		keepOpaque = p.keepOpaque
	}
	if p == nil || !p.returnKey {
		res.Item.Key = nil
		res.Item.WireKey = nil
	} else {
		fallback := []byte(nil)
		if c != nil {
			fallback = c.stripNS(p.key)
		}
		copyMetaUserKey(&res.Item, keepUser, c, fallback, true)
	}
	res.Item.Value = ownValue(keepVal, res.Item.Value)
	if len(res.Item.Opaque) == 0 {
		if keepOpaque != nil {
			res.Item.Opaque = keepOpaque[:0]
		}
	} else if keepOpaque != nil {
		res.Item.Opaque = append(keepOpaque[:0], res.Item.Opaque...)
	}
	out[idx] = res
	if (p == nil || !p.returnKey) && keepWire != nil {
		out[idx].Item.WireKey = keepWire[:0]
	}
}

const metaPipelineIndexThreshold = 16

type metaPipelineIndex struct {
	dense []int32
	num   map[uint64]int
	tok   map[string]int
}

const (
	maxPooledMetaPipelineIndex = 256
	maxDenseMetaOpaque         = 4096
)

var metaPipelineIndexPool = sync.Pool{
	New: func() any { return new(metaPipelineIndex) },
}

func takeMetaPipelineIndex() *metaPipelineIndex {
	index := metaPipelineIndexPool.Get().(*metaPipelineIndex)
	index.dense = index.dense[:0]
	clear(index.num)
	clear(index.tok)
	return index
}

func putMetaPipelineIndex(index *metaPipelineIndex) {
	if index == nil {
		return
	}
	if cap(index.dense) > maxDenseMetaOpaque ||
		len(index.num)+len(index.tok) > maxPooledMetaPipelineIndex {
		return
	}
	clear(index.dense)
	clear(index.num)
	clear(index.tok)
	index.dense = index.dense[:0]
	metaPipelineIndexPool.Put(index)
}

func preparedMetaOpaque(p *preparedMetaOp) (num uint64, numeric bool, tok string, ok bool) {
	if !p.hasTok {
		return 0, false, "", false
	}
	if p.tok == "" {
		return p.tokNum, true, "", true
	}
	n, err := strconv.ParseUint(p.tok, 10, 64)
	if err == nil && opaqueIsCanonical([]byte(p.tok), n) {
		return n, true, "", true
	}
	return 0, false, p.tok, true
}

func samePreparedMetaOpaque(a, b *preparedMetaOp) bool {
	an, anum, atok, aok := preparedMetaOpaque(a)
	bn, bnum, btok, bok := preparedMetaOpaque(b)
	if !aok || !bok || anum != bnum {
		return false
	}
	if anum {
		return an == bn
	}
	return atok == btok
}

func buildMetaPipelineIndex(group []preparedMetaOp) (*metaPipelineIndex, error) {
	if len(group) < metaPipelineIndexThreshold {
		for i := range group {
			for j := i + 1; j < len(group); j++ {
				// #nosec G602 -- j is bounded by len(group).
				if samePreparedMetaOpaque(&group[i], &group[j]) {
					return nil, fmt.Errorf("duplicate opaque: %w", ErrInvalidArgument)
				}
			}
		}
		return nil, nil
	}
	index := takeMetaPipelineIndex()
	maxNum := uint64(0)
	numericCount := 0
	for i := range group {
		num, numeric, _, ok := preparedMetaOpaque(&group[i])
		if ok && numeric {
			numericCount++
			if num > maxNum {
				maxNum = num
			}
		}
	}
	useDense := numericCount > 0 && maxNum < maxDenseMetaOpaque &&
		maxNum+1 <= uint64(len(group))*8
	if useDense {
		need := int(maxNum) + 1
		if cap(index.dense) < need {
			index.dense = make([]int32, need)
		} else {
			index.dense = index.dense[:need]
		}
		for i := range index.dense {
			index.dense[i] = -1
		}
	}
	for i := range group {
		num, numeric, tok, ok := preparedMetaOpaque(&group[i])
		if !ok {
			continue
		}
		if numeric {
			if useDense {
				if index.dense[num] >= 0 {
					putMetaPipelineIndex(index)
					return nil, fmt.Errorf("duplicate opaque: %w", ErrInvalidArgument)
				}
				index.dense[num] = int32(i)
			} else {
				if index.num == nil {
					index.num = make(map[uint64]int, numericCount)
				}
				if _, exists := index.num[num]; exists {
					putMetaPipelineIndex(index)
					return nil, fmt.Errorf("duplicate opaque: %w", ErrInvalidArgument)
				}
				index.num[num] = i
			}
			continue
		}
		if index.tok == nil {
			index.tok = make(map[string]int)
		}
		if _, exists := index.tok[tok]; exists {
			putMetaPipelineIndex(index)
			return nil, fmt.Errorf("duplicate opaque %q: %w", tok, ErrInvalidArgument)
		}
		index.tok[tok] = i
	}
	return index, nil
}

func matchMetaPipelineOpaque(
	header []byte, group []preparedMetaOp, index *metaPipelineIndex,
) (int, bool) {
	num, hasNum, tok := scanMetaOpaque(header)
	if index != nil {
		if hasNum {
			if num < uint64(len(index.dense)) {
				i := index.dense[num]
				return int(i), i >= 0
			}
			i, ok := index.num[num]
			return i, ok
		}
		if len(tok) > 0 {
			i, ok := index.tok[string(tok)]
			return i, ok
		}
		return 0, false
	}
	if hasNum {
		for i := range group {
			n, numeric, _, ok := preparedMetaOpaque(&group[i])
			if ok && numeric && n == num {
				return i, true
			}
		}
	}
	if len(tok) > 0 {
		for i := range group {
			_, numeric, opaque, ok := preparedMetaOpaque(&group[i])
			if ok && !numeric && eqStringBytes(opaque, tok) {
				return i, true
			}
		}
	}
	return 0, false
}

func scanMetaOpaque(header []byte) (num uint64, hasNum bool, tok []byte) {
	line := trimCRLF(header)
	before, after, ok := bytes.Cut(line, []byte{' '})
	if !ok {
		return 0, false, nil
	}
	cd := before
	rest := after
	if bytes.Equal(cd, _VABytes) {
		sp := bytes.IndexByte(rest, ' ')
		if sp < 0 {
			return 0, false, nil
		}
		rest = rest[sp+1:]
	}
	start := 0
	for i := 0; i <= len(rest); i++ {
		if i < len(rest) && rest[i] != ' ' {
			continue
		}
		if i > start && rest[start] == 'O' {
			val := rest[start+1 : i]
			if n, err := parseUintFromBytes(val); err == nil && opaqueIsCanonical(val, n) {
				return n, true, nil
			}
			return 0, false, val
		}
		start = i + 1
	}
	return 0, false, nil
}
