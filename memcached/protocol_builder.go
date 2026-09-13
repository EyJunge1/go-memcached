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
	_EndCRLFBytes     = []byte("END\r\n")
	_StoredCRLFBytes  = []byte("STORED\r\n")
	_DeletedCRLFBytes = []byte("DELETED\r\n")
	_TouchedCRLFBytes = []byte("TOUCHED\r\n")
	_VersionBytes     = []byte("VERSION")

	_MetaMNCRLFBytes = []byte("MN\r\n")
)

// forecastCommonFaultLine maps a well-known status line to a sentinel, or nil.
func forecastCommonFaultLine(line []byte) error {
	hit := mapWireLine(line, wireApp)
	if hit.Class != wireFault || errors.Is(hit.Err, ErrMalformedResponse) {
		return nil
	}
	return hit.Err
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
	bufferPool = sync.Pool{
		New: func() any {
			return bytes.NewBuffer(make([]byte, 0, defaultBufferSize))
		},
	}
	builderPool = sync.Pool{
		New: func() any {
			return &protocolBuilder{
				buf: bufferPool.Get().(*bytes.Buffer),
			}
		},
	}
)

// protocolBuilder is a pooled space-separated command encoder.
type protocolBuilder struct {
	buf *bytes.Buffer
}

// newProtocolBuilder returns a pooled protocolBuilder.
func newProtocolBuilder() *protocolBuilder {
	pb := builderPool.Get().(*protocolBuilder)
	if pb.buf == nil {
		pb.buf = bufferPool.Get().(*bytes.Buffer)
	} else {
		pb.buf.Reset()
	}

	return pb
}

// release returns the builder and its buffer to the pools.
func (b *protocolBuilder) release() {
	if b.buf != nil {
		if b.buf.Cap() <= maxPooledBufferCap {
			b.buf.Reset()
			bufferPool.Put(b.buf)
		}
		b.buf = nil
	}

	builderPool.Put(b)
}

// AddString appends s and a trailing space.
func (b *protocolBuilder) AddString(s string) *protocolBuilder {
	b.buf.WriteString(s)
	b.buf.WriteByte(_SpaceByte)
	return b
}

// AddBytes appends bs and a trailing space.
func (b *protocolBuilder) AddBytes(bs []byte) *protocolBuilder {
	b.buf.Write(bs)
	b.buf.WriteByte(_SpaceByte)
	return b
}

// AddInt appends the decimal form of i and a trailing space.
func (b *protocolBuilder) AddInt(i int) *protocolBuilder {
	b.buf.Write(strconv.AppendInt(b.buf.AvailableBuffer(), int64(i), 10))
	b.buf.WriteByte(_SpaceByte)
	return b
}

// AddUint appends the decimal form of i and a trailing space.
func (b *protocolBuilder) AddUint(i uint64) *protocolBuilder {
	b.buf.Write(strconv.AppendUint(b.buf.AvailableBuffer(), i, 10))
	b.buf.WriteByte(_SpaceByte)
	return b
}

// AddInt64 appends the decimal form of i and a trailing space.
func (b *protocolBuilder) AddInt64(i int64) *protocolBuilder {
	b.buf.Write(strconv.AppendInt(b.buf.AvailableBuffer(), i, 10))
	b.buf.WriteByte(_SpaceByte)
	return b
}

// AddCRLF trims a trailing space and appends CRLF.
func (b *protocolBuilder) AddCRLF() *protocolBuilder {
	if bytes.HasSuffix(b.buf.Bytes(), _SpaceBytes) {
		b.buf.Truncate(b.buf.Len() - 1)
	}

	b.buf.Write(_CRLFBytes)
	return b
}

// AddFlagBool writes flag (with a trailing space) when v is true.
func (b *protocolBuilder) AddFlagBool(flag string, v bool) *protocolBuilder {
	if !v {
		return b
	}

	b.buf.WriteString(flag)
	b.buf.WriteByte(_SpaceByte)
	return b
}

// AddFlagUint writes flag plus tok, including 0 (for example "D0 ").
func (b *protocolBuilder) AddFlagUint(flag string, tok uint64) *protocolBuilder {
	b.buf.WriteString(flag)
	b.buf.Write(strconv.AppendUint(b.buf.AvailableBuffer(), tok, 10))
	b.buf.WriteByte(_SpaceByte)
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
	b.buf.WriteString(flag)
	b.buf.Write(strconv.AppendInt(b.buf.AvailableBuffer(), tok, 10))
	b.buf.WriteByte(_SpaceByte)
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

	b.buf.WriteString(flag)
	b.buf.WriteString(tok)
	b.buf.WriteByte(_SpaceByte)
	return b
}

// AddOpaque writes the O flag from token or from a numeric value.
func (b *protocolBuilder) AddOpaque(token string, num uint64, numSet bool) *protocolBuilder {
	if token != "" {
		return b.AddFlagString("O", token)
	}
	return b.AddFlagUintPresent("O", num, numSet)
}

// build returns a copy of the command bytes, trimming a trailing space and ensuring CRLF.
func (b *protocolBuilder) build() []byte {
	data := b.buf.Bytes()

	result := make([]byte, len(data))
	copy(result, data)

	if bytes.HasSuffix(result, _SpaceBytes) {
		result = result[:len(result)-1]
	}

	if bytes.HasSuffix(result, _CRLFBytes) {
		return result
	}

	result = append(result, _CRLFBytes...)
	return result
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
