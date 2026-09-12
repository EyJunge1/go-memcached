package memcached

import (
	"bytes"
	"fmt"
	"slices"
	"sync"
)

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

const _maxPooledPreparedMeta = 256

var _preparedMetaPool = sync.Pool{
	New: func() any {
		s := make([]preparedMetaOp, 0, 16)
		return &s
	},
}

const (
	_defaultMetaBatchBuffer = 8 << 10
	_maxMetaBatchBuffer     = 64 << 10
)

var _metaBatchBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, _defaultMetaBatchBuffer)
		return &buf
	},
}

func takeMetaBatchBuffer() *[]byte {
	box, ok := _metaBatchBufferPool.Get().(*[]byte)
	if !ok || box == nil {
		buf := make([]byte, 0, _defaultMetaBatchBuffer)
		box = &buf
	}
	*box = (*box)[:0]
	return box
}

func putMetaBatchBuffer(box *[]byte) {
	if box == nil || cap(*box) > _maxMetaBatchBuffer {
		return
	}
	*box = (*box)[:0]
	_metaBatchBufferPool.Put(box)
}

func getPreparedMeta(n int) *[]preparedMetaOp {
	p, ok := _preparedMetaPool.Get().(*[]preparedMetaOp)
	if !ok || p == nil {
		s := make([]preparedMetaOp, 0, 16)
		p = &s
	}
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
	if cap(s) > _maxPooledPreparedMeta {
		return
	}
	*p = s[:0]
	_preparedMetaPool.Put(p)
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
	if c != nil && c.streamCount() > 0 {
		opaque = c.opaqueSeq.Add(1)
	}
	key, keyBox := c.nsKey(op.Key)
	// Prepare* owns routing keys across SetServers/failover remaps (Uber: copy at boundaries).
	if keyBox == nil {
		key = bytes.Clone(key)
	}
	prep := preparedMetaOp{idx: idx, key: key, keyBox: keyBox}

	var err error
	switch {
	case op.Get != nil:
		err = encodeMetaPipelineGet(c, &prep, *op.Get, opaque)
	case op.Set != nil:
		err = encodeMetaPipelineSet(c, &prep, *op.Set, op.Body, opaque)
	case op.Delete != nil:
		err = encodeMetaPipelineDelete(c, &prep, *op.Delete, opaque)
	case op.Arithmetic != nil:
		err = encodeMetaPipelineArithmetic(c, &prep, *op.Arithmetic, op.Delta, opaque)
	case op.Noop != nil:
		err = encodeMetaPipelineNoop(&prep, *op.Noop)
	}
	if err != nil {
		putBuilderBox(keyBox)
		return preparedMetaOp{}, err
	}
	return prep, nil
}

func encodeMetaPipelineGet(c *Client, prep *preparedMetaOp, f MetaGet, opaque uint64) error {
	if err := f.validate(); err != nil {
		return err
	}
	mg := f.toFlags()
	if !mg.Cset {
		mg.q = true
	}
	if err := c.checkKeyValueFlags(prep.key, nil, mg.b); err != nil {
		return err
	}
	if err := ensurePipelineOpaque(&mg, opaque); err != nil {
		return err
	}
	_, line, box := encodeMetaGetOwned(prep.key, &mg)
	prep.cmd = _MgBytes
	prep.line = line
	prep.lineBox = box
	prep.tok = mg.Otoken
	prep.tokNum = mg.O
	prep.hasTok = mg.Oset || mg.OtokenSet
	prep.getFlags = mg
	prep.hasGet = true
	prep.returnKey = f.Return.Key
	return nil
}

func encodeMetaPipelineSet(c *Client, prep *preparedMetaOp, f MetaSet, body []byte, opaque uint64) error {
	ms := f.toFlags()
	ms.q = true
	if err := c.checkKeyValueFlags(prep.key, body, ms.b); err != nil {
		return err
	}
	if err := f.validate(); err != nil {
		return err
	}
	if !validMetaSetMode(ms.M) {
		return fmt.Errorf("invalid meta set mode: %w", ErrInvalidArgument)
	}
	if err := ensureSetOpaque(&ms, opaque); err != nil {
		return err
	}
	header, box, encBody, tail := encodeMetaSetOwned(prep.key, body, &ms)
	prep.cmd = _MsBytes
	prep.line = header
	prep.lineBox = box
	prep.body = encBody
	prep.tail = tail
	prep.valueBinary = ms.b
	prep.tok = ms.Otoken
	prep.tokNum = ms.O
	prep.hasTok = ms.Oset || ms.OtokenSet
	prep.returnKey = f.Return.Key
	return nil
}

func encodeMetaPipelineDelete(c *Client, prep *preparedMetaOp, f MetaDelete, opaque uint64) error {
	if err := f.validate(); err != nil {
		return err
	}
	md := f.toFlags()
	md.q = true
	if err := c.checkKeyValueFlags(prep.key, nil, md.b); err != nil {
		return err
	}
	if err := ensureDeleteOpaque(&md, opaque); err != nil {
		return err
	}
	line, box := encodeMetaDeleteOwned(prep.key, &md)
	prep.cmd = _MdBytes
	prep.line = line
	prep.lineBox = box
	prep.tok = md.Otoken
	prep.tokNum = md.O
	prep.hasTok = md.Oset || md.OtokenSet
	prep.returnKey = f.Return.Key
	return nil
}

func encodeMetaPipelineArithmetic(c *Client, prep *preparedMetaOp, f MetaArithmetic, delta uint64, opaque uint64) error {
	if err := f.validate(); err != nil {
		return err
	}
	ma := f.toFlags()
	if !ma.v {
		ma.q = true
	}
	if err := c.checkKeyValueFlags(prep.key, nil, ma.b); err != nil {
		return err
	}
	if !validMetaArithmeticMode(ma.M) {
		return fmt.Errorf("invalid meta arithmetic mode: %w", ErrInvalidArgument)
	}
	if err := ensureArithOpaque(&ma, opaque); err != nil {
		return err
	}
	line, box := encodeMetaArithmeticOwned(prep.key, delta, &ma)
	prep.cmd = _MaBytes
	prep.line = line
	prep.lineBox = box
	prep.tok = ma.Otoken
	prep.tokNum = ma.O
	prep.hasTok = ma.Oset || ma.OtokenSet
	prep.returnKey = f.Return.Key
	return nil
}

func encodeMetaPipelineNoop(prep *preparedMetaOp, f MetaNoop) error {
	// The batch already ends with mn; another mn would make the barrier reader stop early.
	f.Extra = slices.Clone(f.Extra)
	if err := f.validate(); err != nil {
		return err
	}
	prep.cmd = _MnBytes
	prep.line = nil
	prep.noop = true
	prep.barrier = f
	return nil
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
