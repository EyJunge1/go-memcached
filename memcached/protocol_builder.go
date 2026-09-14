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
	_NotFoundBytes    = []byte("NOT_FOUND")
	_ExistsBytes      = []byte("EXISTS")
	_NotStoredBytes   = []byte("NOT_STORED")
	_VersionBytes     = []byte("VERSION")
	_VersionCmdBytes  = []byte("version")
	_VersionCRLFBytes = []byte("version\r\n")
	_StatsCmdBytes    = []byte("stats")

	_MetaMNCRLFBytes = []byte("MN\r\n")
	_MnCRLFBytes     = []byte("mn\r\n")
	_CRLFMnCRLFBytes = []byte("\r\nmn\r\n")
	_DeletedBytes    = []byte("DELETED")
	_TouchedBytes    = []byte("TOUCHED")
	_OKBytes         = []byte("OK")
	_RESETBytes      = []byte("RESET")
	_STATBytes       = []byte("STAT")
	_MNBytes         = []byte("MN")
	_HDBytes         = []byte("HD")
	_VABytes         = []byte("VA")
	_ENBytes         = []byte("EN")
	_NFBytes         = []byte("NF")
	_NSBytes         = []byte("NS")
	_EXBytes         = []byte("EX")
	_MEBytes         = []byte("ME")
	_HDSpaceBytes    = []byte("HD ")
	_MESpaceBytes    = []byte("ME ")

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
	_DeleteBytes  = []byte("delete")
	_TouchBytes   = []byte("touch")
	_MgBytes      = []byte("mg")
	_MsBytes      = []byte("ms")
	_MdBytes      = []byte("md")
	_MaBytes      = []byte("ma")
	_MnBytes      = []byte("mn")
	_MeBytes      = []byte("me")
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
	case "delete":
		return _DeleteBytes
	case "touch":
		return _TouchBytes
	case "mg":
		return _MgBytes
	case "ms":
		return _MsBytes
	case "md":
		return _MdBytes
	case "ma":
		return _MaBytes
	case "mn":
		return _MnBytes
	case "me":
		return _MeBytes
	default:
		return []byte(command)
	}
}

// forecastCommonFaultLine maps a well-known status line to a sentinel, or nil.
func forecastCommonFaultLine(line []byte) error {
	t := trimCRLF(line)
	if isKnownNominalLine(t) {
		return nil
	}
	switch {
	case bytes.Equal(t, _NotFoundBytes):
		return ErrNotFound
	case bytes.Equal(t, _ExistsBytes):
		return ErrExists
	case bytes.Equal(t, _NotStoredBytes):
		return ErrNotStored
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
		bytes.Equal(t, _DeletedBytes),
		bytes.Equal(t, _TouchedBytes),
		bytes.Equal(t, _OKBytes),
		bytes.Equal(t, _EndBytes),
		bytes.Equal(t, _MNBytes),
		bytes.Equal(t, _HDBytes),
		bytes.Equal(t, _RESETBytes):
		return true
	case bytes.HasPrefix(t, _ValueBytes),
		bytes.HasPrefix(t, _VASpaceBytes),
		bytes.HasPrefix(t, _HDSpaceBytes),
		bytes.HasPrefix(t, _MESpaceBytes),
		bytes.Equal(t, _MEBytes),
		bytes.HasPrefix(t, _VersionBytes),
		bytes.HasPrefix(t, _STATBytes),
		isMetaStatusLine(t),
		isArithmeticLine(t):
		return true
	default:
		return false
	}
}

// isMetaStatusLine reports a two-letter meta CD, optionally followed by flags.
func isMetaStatusLine(t []byte) bool {
	if len(t) < 2 || (len(t) > 2 && t[2] != ' ') {
		return false
	}
	a, b := t[0], t[1]
	return (a == 'H' && b == 'D') ||
		(a == 'V' && b == 'A') ||
		(a == 'E' && b == 'N') ||
		(a == 'N' && b == 'F') ||
		(a == 'N' && b == 'S') ||
		(a == 'E' && b == 'X') ||
		(a == 'M' && b == 'N') ||
		(a == 'M' && b == 'E')
}

// isArithmeticLine reports an incr/decr decimal reply (spaces allowed).
func isArithmeticLine(t []byte) bool {
	seen := false
	for _, c := range t {
		if c == ' ' || c == '\t' {
			continue
		}
		if c < '0' || c > '9' {
			return false
		}
		seen = true
	}
	return seen
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
	const maxServerErrorTextBytes = 4 << 10
	if len(msg) > maxServerErrorTextBytes {
		msg = msg[:maxServerErrorTextBytes]
	}
	return fmt.Errorf("%s: %w", msg, sentinel)
}

const (
	// defaultBufferSize fits get/set/ms plus a max-length key without growing.
	defaultBufferSize = 320

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
	// A bare slice has no pool slot. Stealing one to wrap it drains the pool
	// and can pin a string constant. Drop unboxed raw; boxed paths use rawBox.
	_ = b
}

func putBuilderBox(box *[]byte) {
	if box == nil {
		return
	}
	if cap(*box) < defaultBufferSize || cap(*box) > maxPooledBufferCap {
		return
	}
	*box = (*box)[:0]
	builderBufPool.Put(box)
}

func takeBuilderBox() *[]byte {
	p := builderBufPool.Get().(*[]byte)
	*p = (*p)[:0]
	return p
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

// AddBase64 appends standard Base64(src) and a trailing space.
func (b *protocolBuilder) AddBase64(src []byte) *protocolBuilder {
	p := b.scratch()
	n := base64.StdEncoding.EncodedLen(len(src))
	start := len(*p)
	if cap(*p)-start < n {
		nb := make([]byte, start, start+n+defaultBufferSize)
		copy(nb, *p)
		*p = nb
	}
	*p = (*p)[:start+n]
	base64.StdEncoding.Encode((*p)[start:start+n], src)
	*p = append(*p, _SpaceByte)
	return b
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
	need := max(base64.StdEncoding.DecodedLen(len(src)), 0)
	buf := dst
	if sameByteArray(buf, src) || cap(buf) < need {
		buf = make([]byte, need)
	} else {
		buf = buf[:need]
	}
	n, err := base64.StdEncoding.Decode(buf, src)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}
