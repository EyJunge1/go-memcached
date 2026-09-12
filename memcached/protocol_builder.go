package memcached

import (
	"bytes"
	"encoding/base64"
	"strconv"
	"sync"
)

const (
	// _defaultBufferSize fits get/set/ms plus a max-length key without growing.
	_defaultBufferSize = 320

	// _maxPooledBufferCap stops large writes from pinning huge buffers.
	_maxPooledBufferCap = 4 << 10 // 4 KiB
)

var (
	_emptyBytes = []byte{}

	_builderBufPool = sync.Pool{
		New: func() any {
			b := make([]byte, 0, _defaultBufferSize)
			return &b
		},
	}
	_builderPool = sync.Pool{
		New: func() any {
			return &protocolBuilder{}
		},
	}
)

// newProtocolBuilder returns a pooled protocolBuilder.
func newProtocolBuilder() *protocolBuilder {
	pb, ok := _builderPool.Get().(*protocolBuilder)
	if !ok || pb == nil {
		pb = &protocolBuilder{}
	}
	_ = pb.scratch()
	return pb
}

// protocolBuilder is a pooled space-separated command encoder.
type protocolBuilder struct {
	box *[]byte
}

func (b *protocolBuilder) buf() []byte {
	if b.box == nil {
		return nil
	}
	return *b.box
}

func (b *protocolBuilder) scratch() *[]byte {
	if b.box == nil {
		p, ok := _builderBufPool.Get().(*[]byte)
		if !ok || p == nil {
			buf := make([]byte, 0, _defaultBufferSize)
			p = &buf
		}
		*p = (*p)[:0]
		b.box = p
	}
	return b.box
}

// addMetaKey writes key, base64-encoded when binary is set.
func (b *protocolBuilder) addMetaKey(key []byte, binary bool) {
	if binary {
		b.AddBase64(key)
		return
	}
	b.AddBytes(key)
}

// release returns the builder and its buffer to the pools.
func (b *protocolBuilder) release() {
	if b.box != nil {
		*b.box = (*b.box)[:0]
		if cap(*b.box) <= _maxPooledBufferCap {
			_builderBufPool.Put(b.box)
		}
		b.box = nil
	}

	_builderPool.Put(b)
}

// AddString appends s and a trailing space.
func (b *protocolBuilder) AddString(s string) *protocolBuilder {
	p := b.scratch()
	*p = append(*p, s...)
	*p = append(*p, _SpaceByte)
	return b
}

// AddBytes appends bs and a trailing space.
func (b *protocolBuilder) AddBytes(bs []byte) *protocolBuilder {
	p := b.scratch()
	*p = append(*p, bs...)
	*p = append(*p, _SpaceByte)
	return b
}

// AddBase64 appends standard Base64(src) and a trailing space.
func (b *protocolBuilder) AddBase64(src []byte) *protocolBuilder {
	p := b.scratch()
	n := base64.StdEncoding.EncodedLen(len(src))
	start := len(*p)
	if cap(*p)-start < n {
		nb := make([]byte, start, start+n+_defaultBufferSize)
		copy(nb, *p)
		*p = nb
	}
	*p = (*p)[:start+n]
	base64.StdEncoding.Encode((*p)[start:start+n], src)
	*p = append(*p, _SpaceByte)
	return b
}

// AddInt appends the decimal form of i and a trailing space.
func (b *protocolBuilder) AddInt(i int) *protocolBuilder {
	p := b.scratch()
	*p = strconv.AppendInt(*p, int64(i), 10)
	*p = append(*p, _SpaceByte)
	return b
}

// AddUint appends the decimal form of i and a trailing space.
func (b *protocolBuilder) AddUint(i uint64) *protocolBuilder {
	p := b.scratch()
	*p = strconv.AppendUint(*p, i, 10)
	*p = append(*p, _SpaceByte)
	return b
}

// AddInt64 appends the decimal form of i and a trailing space.
func (b *protocolBuilder) AddInt64(i int64) *protocolBuilder {
	p := b.scratch()
	*p = strconv.AppendInt(*p, i, 10)
	*p = append(*p, _SpaceByte)
	return b
}

// AddCRLF trims a trailing space and appends CRLF.
func (b *protocolBuilder) AddCRLF() *protocolBuilder {
	p := b.scratch()
	if bytes.HasSuffix(*p, _SpaceBytes) {
		*p = (*p)[:len(*p)-1]
	}

	*p = append(*p, _CRLFBytes...)
	return b
}

// AddFlagBool writes flag (with a trailing space) when v is true.
func (b *protocolBuilder) AddFlagBool(flag string, v bool) *protocolBuilder {
	if !v {
		return b
	}

	p := b.scratch()
	*p = append(*p, flag...)
	*p = append(*p, _SpaceByte)
	return b
}

// AddFlagUint writes flag plus tok, including 0 (for example "D0 ").
func (b *protocolBuilder) AddFlagUint(flag string, tok uint64) *protocolBuilder {
	p := b.scratch()
	*p = append(*p, flag...)
	*p = strconv.AppendUint(*p, tok, 10)
	*p = append(*p, _SpaceByte)
	return b
}

// AddFlagUintPresent writes the flag when set is true or tok is non-zero.
func (b *protocolBuilder) AddFlagUintPresent(flag string, tok uint64, set bool) *protocolBuilder {
	if !set && tok == 0 {
		return b
	}
	return b.AddFlagUint(flag, tok)
}

// AddFlagInt64Present writes a signed token when set (meta T/N/R, including -1).
func (b *protocolBuilder) AddFlagInt64Present(flag string, tok int64, set bool) *protocolBuilder {
	if !set && tok == 0 {
		return b
	}
	p := b.scratch()
	*p = append(*p, flag...)
	*p = strconv.AppendInt(*p, tok, 10)
	*p = append(*p, _SpaceByte)
	return b
}

// AddFlagUintNonZero writes the flag only when tok is non-zero (F0 is omitted).
func (b *protocolBuilder) AddFlagUintNonZero(flag string, tok uint64) *protocolBuilder {
	if tok == 0 {
		return b
	}
	return b.AddFlagUint(flag, tok)
}

// AddFlagString writes flag plus tok when tok is non-empty.
func (b *protocolBuilder) AddFlagString(flag, tok string) *protocolBuilder {
	if tok == "" {
		return b
	}

	p := b.scratch()
	*p = append(*p, flag...)
	*p = append(*p, tok...)
	*p = append(*p, _SpaceByte)
	return b
}

// AddOpaque writes the O flag from token or from a numeric value.
func (b *protocolBuilder) AddOpaque(token string, num uint64, numSet bool) *protocolBuilder {
	if token != "" {
		return b.AddFlagString("O", token)
	}
	return b.AddFlagUintPresent("O", num, numSet)
}

func (b *protocolBuilder) finalize() {
	p := b.scratch()
	if bytes.HasSuffix(*p, _SpaceBytes) {
		*p = (*p)[:len(*p)-1]
	}
	if !bytes.HasSuffix(*p, _CRLFBytes) {
		*p = append(*p, _CRLFBytes...)
	}
}

// takeBytes detaches the command bytes and the pool slot. Call at most once.
func (b *protocolBuilder) takeBytes() (raw []byte, box *[]byte) {
	if b.box == nil {
		return nil, nil
	}
	b.finalize()
	raw = *b.box
	box = b.box
	b.box = nil
	return raw, box
}

// build detaches the command bytes. Call at most once; release then has no buffer.
func (b *protocolBuilder) build() []byte {
	raw, _ := b.takeBytes()
	return raw
}

// request builds a pooled request that returns the command buffer on release.
func (b *protocolBuilder) request(cmd, key []byte) *request {
	if b.box == nil {
		return buildRequest(cmd, key, nil)
	}
	b.finalize()
	req := buildRequest(cmd, key, *b.box)
	req.rawBox = b.box
	b.box = nil
	return req
}

func putBuilderBuf(b []byte) {
	// A bare slice has no pool slot. Stealing one to wrap it drains the pool
	// and can pin a string constant. Drop unboxed raw; boxed paths use rawBox.
	_ = b
}

func putBuilderBox(box *[]byte) {
	if box == nil {
		return
	}
	if cap(*box) < _defaultBufferSize || cap(*box) > _maxPooledBufferCap {
		return
	}
	*box = (*box)[:0]
	_builderBufPool.Put(box)
}

func takeBuilderBox() *[]byte {
	p, ok := _builderBufPool.Get().(*[]byte)
	if !ok || p == nil {
		b := make([]byte, 0, _defaultBufferSize)
		p = &b
	}
	*p = (*p)[:0]
	return p
}

// eqStringBytes compares s and b without allocating.
func eqStringBytes(s string, b []byte) bool {
	if len(s) != len(b) {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] != b[i] {
			return false
		}
	}
	return true
}

// trimCRLF strips a trailing CRLF or LF from line.
// Always returns a non-nil slice (empty when input is nil/empty) so callers can
// safely slice or compare without NilAway false positives on nil flows.
func trimCRLF(line []byte) []byte {
	if len(line) == 0 {
		return _emptyBytes
	}
	if n := len(line); n >= 2 && line[n-2] == '\r' && line[n-1] == '\n' {
		return line[:n-2]
	}
	if n := len(line); n >= 1 && line[n-1] == '\n' {
		return line[:n-1]
	}
	return line
}

// isMetaNoopLine reports whether line is a meta MN reply.
func isMetaNoopLine(line []byte) bool {
	return bytes.Equal(trimCRLF(line), _MNBytes)
}

// base64Encode encodes src with standard Base64.
func base64Encode(src []byte) []byte {
	dst := make([]byte, base64.StdEncoding.EncodedLen(len(src)))
	base64.StdEncoding.Encode(dst, src)
	return dst
}

// base64Decode decodes src with standard Base64.
func base64Decode(src []byte) ([]byte, error) {
	return base64DecodeInto(nil, src)
}

// base64DecodeInto decodes src into dst when cap is enough and dst does not
// share backing with src (WireKey). Otherwise it allocates.
func base64DecodeInto(dst, src []byte) ([]byte, error) {
	if len(src) == 0 {
		if dst != nil {
			return dst[:0], nil
		}
		return nil, nil
	}
	need := base64.StdEncoding.DecodedLen(len(src))
	buf := dst
	if buf != nil && !sameByteArray(buf, src) && cap(buf) >= need {
		buf = buf[:need]
	} else {
		buf = make([]byte, need)
	}
	n, err := base64.StdEncoding.Decode(buf, src)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}
