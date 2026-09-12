package memcached

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
)

type protocolReader interface {
	readLine(delim byte) ([]byte, error)
	readExact(n int) ([]byte, error)
}

// payloadReader is an in-memory protocolReader for tests.
type payloadReader struct {
	r *bufio.Reader
}

var (
	_ protocolReader        = (*payloadReader)(nil)
	_ protocolReader        = (*byteFrame)(nil)
	_ protocolReader        = wireProtocolReader{}
	_ sliceReader           = (*payloadReader)(nil)
	_ sliceReader           = (*byteFrame)(nil)
	_ framedValueIntoReader = (*payloadReader)(nil)
	_ framedValueIntoReader = (*byteFrame)(nil)
)

// newPayloadReader wraps payload for line and exact reads.
func newPayloadReader(payload []byte) *payloadReader {
	return &payloadReader{r: bufio.NewReader(bytes.NewReader(payload))}
}

// readLine reads through the next delim.
func (p *payloadReader) readLine(delim byte) ([]byte, error) {
	return p.r.ReadBytes(delim)
}

// readSlice returns a line that is only valid until the next read.
func (p *payloadReader) readSlice(delim byte) ([]byte, error) {
	line, err := p.r.ReadSlice(delim)
	if errors.Is(err, bufio.ErrBufferFull) {
		return p.r.ReadBytes(delim)
	}
	return line, err
}

// readExact reads exactly n bytes.
func (p *payloadReader) readExact(n int) ([]byte, error) {
	if n < 0 {
		return nil, fmt.Errorf("negative read: %w", ErrMalformedResponse)
	}
	if n == 0 {
		return nil, nil
	}
	scratch := getSizedBytes(n)
	buf := scratch[:n]
	if err := readFullFast(p.r, buf); err != nil {
		releaseBytes(scratch)
		return nil, err
	}
	return buf, nil
}

func (p *payloadReader) readFramedValueInto(dst []byte) error {
	return readFramedValueIntoFrom(p.r, dst)
}

func (p *payloadReader) buffered() int {
	if p == nil || p.r == nil {
		return 0
	}
	return p.r.Buffered()
}

// byteFrame is an in-memory protocolReader over an assembled UDP payload.
// Lines from readSlice are views into buf and are only valid until release.
type byteFrame struct {
	buf []byte
	off int
}

func (f *byteFrame) readLine(delim byte) ([]byte, error) {
	line, err := f.readSlice(delim)
	if err != nil {
		if len(line) == 0 {
			return nil, err
		}
		return bytes.Clone(line), err
	}
	return bytes.Clone(line), nil
}

func (f *byteFrame) readSlice(delim byte) ([]byte, error) {
	if f.off >= len(f.buf) {
		return nil, io.EOF
	}
	i := bytes.IndexByte(f.buf[f.off:], delim)
	if i < 0 {
		line := f.buf[f.off:]
		f.off = len(f.buf)
		return line, io.EOF
	}
	end := f.off + i + 1
	line := f.buf[f.off:end]
	f.off = end
	return line, nil
}

func (f *byteFrame) readExact(n int) ([]byte, error) {
	if n < 0 {
		return nil, fmt.Errorf("negative read: %w", ErrMalformedResponse)
	}
	if f.off+n > len(f.buf) {
		return nil, io.ErrUnexpectedEOF
	}
	if n == 0 {
		f.off += n
		return nil, nil
	}
	scratch := getSizedBytes(n)
	out := scratch[:n]
	copy(out, f.buf[f.off:f.off+n])
	f.off += n
	return out, nil
}

func (f *byteFrame) readFramedValueInto(dst []byte) error {
	need := len(dst) + 2
	if f.off+need > len(f.buf) {
		return io.ErrUnexpectedEOF
	}
	copy(dst, f.buf[f.off:f.off+len(dst)])
	crlf := f.buf[f.off+len(dst) : f.off+need]
	f.off += need
	if crlf[0] != '\r' || crlf[1] != '\n' {
		return fmt.Errorf("data block missing CRLF: %w", ErrMalformedResponse)
	}
	return nil
}

func (f *byteFrame) buffered() int {
	if f == nil {
		return 0
	}
	n := len(f.buf) - f.off
	if n < 0 {
		return 0
	}
	return n
}

type wireProtocolReader struct{ c wireConn }

func (w wireProtocolReader) readLine(delim byte) ([]byte, error) {
	return w.c.readLine(delim)
}

func (w wireProtocolReader) readExact(n int) ([]byte, error) {
	return w.c.readExact(n)
}
