package memcached

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
)

var (
	_ protocolReader        = (*conn)(nil)
	_ sliceReader           = (*conn)(nil)
	_ framedValueIntoReader = (*conn)(nil)
)

func (c *conn) readLine(delim byte) ([]byte, error) {
	if c.isClosed() {
		return nil, errConnClosed
	}
	if c.rr == nil {
		return nil, fmt.Errorf("udp stream read: %w", ErrNotSupported)
	}
	return readLimitedLine(c.rr, delim, _protocolLineLimit)
}

// readSlice returns a line that is only valid until the next read.
func (c *conn) readSlice(delim byte) ([]byte, error) {
	if c.isClosed() {
		return nil, errConnClosed
	}
	if c.rr == nil {
		return nil, fmt.Errorf("udp stream read: %w", ErrNotSupported)
	}
	line, err := c.rr.ReadSlice(delim)
	if err != nil {
		if errors.Is(err, bufio.ErrBufferFull) {
			return readLimitedLineContinue(c.rr, delim, _protocolLineLimit, line)
		}
		return line, err
	}
	if len(line) > _protocolLineLimit {
		return nil, fmt.Errorf("protocol line too long: %w", ErrMalformedResponse)
	}
	return line, nil
}

// readExact reads exactly n bytes into a sized-pool buffer for VALUE/VA bodies.
// Callers that discard the slice without retaining it may releaseBytes.
func (c *conn) readExact(n int) ([]byte, error) {
	if c.isClosed() {
		return nil, errConnClosed
	}
	if c.rr == nil {
		return nil, fmt.Errorf("udp stream read: %w", ErrNotSupported)
	}
	if n < 0 {
		return nil, fmt.Errorf("negative read: %w", ErrMalformedResponse)
	}
	if n == 0 {
		return nil, nil
	}
	scratch := getSizedBytes(n)
	buf := scratch[:n]
	if err := readFullFast(c.rr, buf); err != nil {
		releaseBytes(scratch)
		return nil, err
	}
	return buf, nil
}

func (c *conn) readFramedValueInto(dst []byte) error {
	if c.isClosed() {
		return errConnClosed
	}
	if c.rr == nil {
		return fmt.Errorf("udp stream read: %w", ErrNotSupported)
	}
	return readFramedValueIntoBufio(c.rr, dst)
}

// readDatagram reads one UDP datagram. The slice is valid until the next
// readDatagram or Close on this conn; assembleUDP copies what it keeps.
func (c *conn) readDatagram() ([]byte, error) {
	if c.isClosed() {
		return nil, errConnClosed
	}
	if c.udpIn == nil {
		c.udpIn = make([]byte, 65535)
	}
	n, err := c.raw.Read(c.udpIn)
	if err != nil {
		return nil, err
	}
	return c.udpIn[:n], nil
}

// Read implements io.Reader on the buffered socket.
func (c *conn) Read(p []byte) (n int, err error) {
	if c.isClosed() {
		return 0, errConnClosed
	}
	if c.rr == nil {
		return c.raw.Read(p)
	}

	return c.rr.Read(p)
}

// readLimitedLine reads through delim with a hard byte cap.
func readLimitedLine(r *bufio.Reader, delim byte, limit int) ([]byte, error) {
	line, err := r.ReadSlice(delim)
	if err != nil {
		if errors.Is(err, bufio.ErrBufferFull) {
			return readLimitedLineContinue(r, delim, limit, line)
		}
		if len(line) == 0 {
			return nil, err
		}
		if len(line) > limit {
			return nil, fmt.Errorf("protocol line too long: %w", ErrMalformedResponse)
		}
		return bytes.Clone(line), err
	}
	if len(line) > limit {
		return nil, fmt.Errorf("protocol line too long: %w", ErrMalformedResponse)
	}
	return bytes.Clone(line), nil
}

func readLimitedLineContinue(r *bufio.Reader, delim byte, limit int, first []byte) ([]byte, error) {
	if len(first) >= limit {
		return nil, fmt.Errorf("protocol line too long: %w", ErrMalformedResponse)
	}
	buf := make([]byte, len(first), min(limit, max(len(first)*2, 64)))
	copy(buf, first)
	for {
		line, err := r.ReadSlice(delim)
		if len(line) > 0 {
			if len(buf)+len(line) > limit {
				return nil, fmt.Errorf("protocol line too long: %w", ErrMalformedResponse)
			}
			buf = append(buf, line...)
		}
		if err == nil {
			return buf, nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			if len(buf) >= limit {
				return nil, fmt.Errorf("protocol line too long: %w", ErrMalformedResponse)
			}
			continue
		}
		if len(buf) > 0 && errors.Is(err, io.EOF) {
			return buf, io.EOF
		}
		if len(buf) == 0 {
			return nil, err
		}
		return buf, err
	}
}

// readFullFast is a specialized io.ReadFull: one Read that returns exactly
// len(buf) bytes succeeds immediately.
func readFullFast(r io.Reader, buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	n, err := r.Read(buf)
	if n == len(buf) {
		return nil
	}
	if n > 0 {
		_, err = io.ReadFull(r, buf[n:])
		return err
	}
	if err != nil {
		return err
	}
	_, err = io.ReadFull(r, buf)
	return err
}
