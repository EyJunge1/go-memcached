package memcached

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"sync"
)

const (
	maxKeySize           = 250
	defaultMaxValueBytes = 1 << 20
	maxValueSize         = defaultMaxValueBytes
)

var (
	_SpaceBytes    = []byte{' '}
	_SpaceByte     = byte(' ')
	_CRLFBytes     = []byte("\r\n")
	_NoReplyBytes  = []byte("noreply")
	_QuitCRLFBytes = []byte("quit\r\n")

	_OKCRLFBytes      = []byte("OK\r\n")
	_ValueBytes       = []byte("VALUE")
	_ValueSpaceBytes  = []byte("VALUE ")
	_VASpaceBytes     = []byte("VA ")
	_EndBytes         = []byte("END")
	_EndCRLFBytes     = []byte("END\r\n")
	_StoredBytes      = []byte("STORED")
	_StoredCRLFBytes  = []byte("STORED\r\n")
	_DeletedCRLFBytes = []byte("DELETED\r\n")
	_TouchedCRLFBytes = []byte("TOUCHED\r\n")
	_VersionBytes     = []byte("VERSION")

	_MetaMNCRLFBytes = []byte("MN\r\n")

	_GetBytes     = []byte("get")
	_GetsBytes    = []byte("gets")
	_GatBytes     = []byte("gat")
	_GatsBytes    = []byte("gats")
	_SetBytes     = []byte("set")
	_AddBytes     = []byte("add")
	_ReplaceBytes = []byte("replace")
	_AppendBytes  = []byte("append")
	_PrependBytes = []byte("prepend")
	_CasBytes     = []byte("cas")
	_IncrBytes    = []byte("incr")
	_DecrBytes    = []byte("decr")
)

// verbBytes returns a interned command verb, or a copy for unknown verbs.
func verbBytes(command string) []byte {
	switch command {
	case "get":
		return _GetBytes
	case "gets":
		return _GetsBytes
	case "gat":
		return _GatBytes
	case "gats":
		return _GatsBytes
	case "set":
		return _SetBytes
	case "add":
		return _AddBytes
	case "replace":
		return _ReplaceBytes
	case "append":
		return _AppendBytes
	case "prepend":
		return _PrependBytes
	case "cas":
		return _CasBytes
	case "incr":
		return _IncrBytes
	case "decr":
		return _DecrBytes
	default:
		return []byte(command)
	}
}

// forecastCommonFaultLine maps a well-known status line to a sentinel, or nil.
func forecastCommonFaultLine(line []byte) error {
	if isKnownNominalLine(trimCRLF(line)) {
		return nil
	}
	hit := mapWireLine(line, wireApp)
	if hit.Class != wireFault || errors.Is(hit.Err, ErrMalformedResponse) {
		return nil
	}
	return hit.Err
}

// isKnownNominalLine is true for success and data headers that must not be
// stringified just to decide they are not faults.
func isKnownNominalLine(t []byte) bool {
	switch {
	case bytes.Equal(t, _StoredBytes),
		bytes.Equal(t, []byte("DELETED")),
		bytes.Equal(t, []byte("TOUCHED")),
		bytes.Equal(t, []byte("OK")),
		bytes.Equal(t, _EndBytes),
		bytes.Equal(t, []byte("MN")),
		bytes.Equal(t, []byte("HD")),
		bytes.Equal(t, []byte("RESET")):
		return true
	case bytes.HasPrefix(t, _ValueBytes),
		bytes.HasPrefix(t, _VASpaceBytes),
		bytes.HasPrefix(t, []byte("HD ")),
		bytes.HasPrefix(t, []byte("ME ")),
		bytes.Equal(t, []byte("ME")),
		bytes.HasPrefix(t, _VersionBytes),
		bytes.HasPrefix(t, []byte("STAT")):
		return true
	default:
		return false
	}
}

// prefixedProtocolError maps a prefix-matched status line onto sentinel.
func prefixedProtocolError(line []byte, prefix string, sentinel error) error {
	trimmed := trimCRLF(line)
	p := []byte(prefix)
	if !bytes.HasPrefix(trimmed, p) {
		return nil
	}
	rest := trimmed[len(p):]
	if len(rest) > 0 && rest[0] != ' ' && rest[0] != ':' {
		return nil
	}
	msg := bytes.TrimSpace(bytes.TrimLeft(rest, " :"))
	return fmt.Errorf("%s: %w", msg, sentinel)
}

const (
	// defaultBufferSize fits typical short headers; larger frames grow once.
	defaultBufferSize = 64

	// maxPooledBufferCap stops large writes from pinning huge buffers.
	maxPooledBufferCap = 4 << 10 // 4 KiB
)

var (
	builderBufPool = sync.Pool{
		New: func() any {
			b := make([]byte, 0, defaultBufferSize)
			return &b
		},
	}
	builderPool = sync.Pool{
		New: func() any {
			return &protocolBuilder{}
		},
	}
)

func putBuilderBuf(b []byte) {
	if cap(b) < defaultBufferSize || cap(b) > maxPooledBufferCap {
		return
	}
	p := builderBufPool.Get().(*[]byte)
	*p = b[:0]
	builderBufPool.Put(p)
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
		p := builderBufPool.Get().(*[]byte)
		*p = (*p)[:0]
		b.box = p
	}
	return b.box
}

// newProtocolBuilder returns a pooled protocolBuilder.
func newProtocolBuilder() *protocolBuilder {
	pb := builderPool.Get().(*protocolBuilder)
	_ = pb.scratch()
	return pb
}

// release returns the builder and its buffer to the pools.
func (b *protocolBuilder) release() {
	if b.box != nil {
		*b.box = (*b.box)[:0]
		if cap(*b.box) <= maxPooledBufferCap {
			builderBufPool.Put(b.box)
		}
		b.box = nil
	}

	builderPool.Put(b)
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

// build detaches the command bytes. Call at most once; release then has no buffer.
func (b *protocolBuilder) build() []byte {
	if b.box == nil {
		return nil
	}
	b.finalize()
	out := *b.box
	b.box = nil
	return out
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

// trimCRLF strips a trailing CRLF or LF from line.
func trimCRLF(line []byte) []byte {
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
	return bytes.Equal(trimCRLF(line), []byte("MN"))
}

// base64Encode encodes src with standard Base64.
func base64Encode(src []byte) []byte {
	dst := make([]byte, base64.StdEncoding.EncodedLen(len(src)))
	base64.StdEncoding.Encode(dst, src)
	return dst
}

// base64Decode decodes src with standard Base64.
func base64Decode(src []byte) ([]byte, error) {
	dst := make([]byte, base64.StdEncoding.DecodedLen(len(src)))
	n, err := base64.StdEncoding.Decode(dst, src)
	if err != nil {
		return nil, err
	}
	return dst[:n], nil
}
