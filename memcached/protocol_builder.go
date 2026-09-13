package memcached

import (
	"bytes"
	"encoding/base64"
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

// forecastCommonFaultLine forecasts the error line from the response line.
// ERROR\r\n
// CLIENT_ERROR <message>\r\n
// SERVER_ERROR <message>\r\n
// NOT_FOUND\r\n
// EXISTS\r\n
// NOT_STORED\r\n
// EN <flags>*\r\n or EN\r\n
// EX <flags>*\r\n or EX\r\n
// NS <flags>*\r\n or NS\r\n
// NF <flags>*\r\n or NF\r\n
func forecastCommonFaultLine(line []byte) error {
	if err := prefixedProtocolError(line, "CLIENT_ERROR", ErrClientError); err != nil {
		return err
	}
	if err := prefixedProtocolError(line, "SERVER_ERROR", ErrServerError); err != nil {
		return err
	}
	switch {
	case bytes.Equal(trimCRLF(line), []byte("ERROR")):
		return ErrNonexistentCommand
	case bytes.Equal(trimCRLF(line), []byte("NOT_FOUND")):
		return ErrNotFound
	case bytes.Equal(trimCRLF(line), []byte("EXISTS")):
		return ErrExists
	case bytes.Equal(trimCRLF(line), []byte("NOT_STORED")):
		return ErrNotStored
	// meta status codes: exact token or token plus flags, never prefixes of END.
	case metaStatusCode(line, "NF"):
		return ErrNotFound
	case metaStatusCode(line, "NS"):
		return ErrNotStored
	case metaStatusCode(line, "EX"):
		return ErrExists
	case metaStatusCode(line, "EN"):
		return ErrNotFound
	}

	return nil
}

func prefixedProtocolError(line []byte, prefix string, sentinel error) error {
	trimmed := trimCRLF(line)
	p := []byte(prefix)
	if !bytes.HasPrefix(trimmed, p) {
		return nil
	}
	rest := trimmed[len(p):]
	if len(rest) > 0 && rest[0] != ' ' {
		return nil
	}
	return fmt.Errorf("%s: %w", bytes.TrimSpace(rest), sentinel)
}

func metaStatusCode(line []byte, code string) bool {
	trimmed := trimCRLF(line)
	if bytes.Equal(trimmed, []byte(code)) {
		return true
	}
	return bytes.HasPrefix(trimmed, []byte(code+" "))
}

const (
	// defaultBufferSize covers common short text/meta command headers and
	// small payloads without growth. Measured lengths with the builder's
	// spacing rules: get/delete ≈14-17B, small set ≈32B, meta set with
	// several flags ≈42B. Larger storage values grow once; oversized
	// buffers are discarded on release via maxPooledBufferCap.
	defaultBufferSize = 64

	// maxPooledBufferCap prevents large storage values from permanently
	// retaining oversized buffers in the pool.
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

// The protocolBuilder is used to build a protocol message.
// We can use it build request command quickly with chaining method like this:
//
// newProtocolBuilder().
// AddString("set").AddString("key").AddInt(0).AddInt(0).AddInt(5).NewLine().
// AddString("value").build()
//
// The result is:
//
// set key 0 0 5\r\n
// value\r\n
type protocolBuilder struct {
	buf *bytes.Buffer
}

func newProtocolBuilder() *protocolBuilder {
	pb := builderPool.Get().(*protocolBuilder)
	if pb.buf == nil {
		pb.buf = bufferPool.Get().(*bytes.Buffer)
	} else {
		pb.buf.Reset()
	}

	return pb
}

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

func (b *protocolBuilder) AddString(s string) *protocolBuilder {
	b.buf.WriteString(s)
	b.buf.WriteByte(_SpaceByte)
	return b
}

func (b *protocolBuilder) AddBytes(bs []byte) *protocolBuilder {
	b.buf.Write(bs)
	b.buf.WriteByte(_SpaceByte)
	return b
}

func (b *protocolBuilder) AddInt(i int) *protocolBuilder {
	b.buf.Write(strconv.AppendInt(b.buf.AvailableBuffer(), int64(i), 10))
	b.buf.WriteByte(_SpaceByte)
	return b
}

func (b *protocolBuilder) AddUint(i uint64) *protocolBuilder {
	b.buf.Write(strconv.AppendUint(b.buf.AvailableBuffer(), i, 10))
	b.buf.WriteByte(_SpaceByte)
	return b
}

func (b *protocolBuilder) AddInt64(i int64) *protocolBuilder {
	b.buf.Write(strconv.AppendInt(b.buf.AvailableBuffer(), i, 10))
	b.buf.WriteByte(_SpaceByte)
	return b
}

func (b *protocolBuilder) AddCRLF() *protocolBuilder {
	// trim space if needed
	if bytes.HasSuffix(b.buf.Bytes(), _SpaceBytes) {
		b.buf.Truncate(b.buf.Len() - 1)
	}

	b.buf.Write(_CRLFBytes)
	return b
}

// AddFlagBool adds a flag with string value to the protocol message.
// e.g. AddFlagBool("k", true) will append "k" to the protocol message.
func (b *protocolBuilder) AddFlagBool(flag string, v bool) *protocolBuilder {
	if !v {
		return b
	}

	b.buf.WriteString(flag)
	b.buf.WriteByte(_SpaceByte)
	return b
}

// AddFlagUint adds a flag with uint64 value, including token 0 (e.g. "D0 ").
func (b *protocolBuilder) AddFlagUint(flag string, tok uint64) *protocolBuilder {
	b.buf.WriteString(flag)
	b.buf.Write(strconv.AppendUint(b.buf.AvailableBuffer(), tok, 10))
	b.buf.WriteByte(_SpaceByte)
	return b
}

// AddFlagUintPresent writes the flag when it was set or the token is non-zero.
func (b *protocolBuilder) AddFlagUintPresent(flag string, tok uint64, set bool) *protocolBuilder {
	if !set && tok == 0 {
		return b
	}
	return b.AddFlagUint(flag, tok)
}

// AddFlagInt64Present writes a signed flag token when set (Meta T/N/R, including -1).
func (b *protocolBuilder) AddFlagInt64Present(flag string, tok int64, set bool) *protocolBuilder {
	if !set && tok == 0 {
		return b
	}
	b.buf.WriteString(flag)
	b.buf.Write(strconv.AppendInt(b.buf.AvailableBuffer(), tok, 10))
	b.buf.WriteByte(_SpaceByte)
	return b
}

// AddFlagUintNonZero writes the flag only when tok != 0 (used for F0 omit).
func (b *protocolBuilder) AddFlagUintNonZero(flag string, tok uint64) *protocolBuilder {
	if tok == 0 {
		return b
	}
	return b.AddFlagUint(flag, tok)
}

func (b *protocolBuilder) AddFlagString(flag, tok string) *protocolBuilder {
	if tok == "" {
		return b
	}

	b.buf.WriteString(flag)
	b.buf.WriteString(tok)
	b.buf.WriteByte(_SpaceByte)
	return b
}

func (b *protocolBuilder) AddOpaque(token string, num uint64, numSet bool) *protocolBuilder {
	if token != "" {
		return b.AddFlagString("O", token)
	}
	return b.AddFlagUintPresent("O", num, numSet)
}

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

func trimCRLF(line []byte) []byte {
	if n := len(line); n >= 2 && line[n-2] == '\r' && line[n-1] == '\n' {
		return line[:n-2]
	}
	if n := len(line); n >= 1 && line[n-1] == '\n' {
		return line[:n-1]
	}
	return line
}

func base64Encode(src []byte) []byte {
	dst := make([]byte, base64.StdEncoding.EncodedLen(len(src)))
	base64.StdEncoding.Encode(dst, src)
	return dst
}

func base64Decode(src []byte) ([]byte, error) {
	dst := make([]byte, base64.StdEncoding.DecodedLen(len(src)))
	n, err := base64.StdEncoding.Decode(dst, src)
	if err != nil {
		return nil, err
	}
	return dst[:n], nil
}
