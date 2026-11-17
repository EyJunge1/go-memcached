package memcached

import (
	"bytes"
	"io"
	"testing"
)

func TestPayloadReaderReadExact(t *testing.T) {
	p := newPayloadReader([]byte("abcdef"))
	got, err := p.readExact(3)
	noError(t, err)
	equal(t, []byte("abc"), got)
	releaseBytes(got)

	empty, err := p.readExact(0)
	noError(t, err)
	if empty != nil {
		t.Fatalf("empty readExact want nil, got %q", empty)
	}

	_, err = p.readExact(-1)
	isErrorIs(t, err, ErrMalformedResponse)

	_, err = p.readExact(10)
	if err == nil {
		t.Fatal("expected short-buffer error")
	}
}

func TestPayloadReaderReadLineAndBuffered(t *testing.T) {
	p := newPayloadReader([]byte("one\ntwo\n"))
	line, err := p.readLine('\n')
	noError(t, err)
	equal(t, []byte("one\n"), line)
	if p.buffered() <= 0 {
		t.Fatal("expected buffered bytes")
	}
	line, err = p.readSlice('\n')
	noError(t, err)
	equal(t, []byte("two\n"), line)
	equal(t, 0, (*payloadReader)(nil).buffered())
}

func TestByteFrameReads(t *testing.T) {
	f := &byteFrame{buf: []byte("HD\r\nva")}
	line, err := f.readLine('\n')
	noError(t, err)
	equal(t, []byte("HD\r\n"), line)

	chunk, err := f.readExact(2)
	noError(t, err)
	equal(t, []byte("va"), chunk)
	releaseBytes(chunk)

	_, err = f.readExact(1)
	isErrorIs(t, err, io.ErrUnexpectedEOF)

	f2 := &byteFrame{buf: []byte("no-delim")}
	rest, err := f2.readSlice('\n')
	isErrorIs(t, err, io.EOF)
	equal(t, []byte("no-delim"), rest)

	_, err = (&byteFrame{buf: []byte("x")}).readExact(-1)
	isErrorIs(t, err, ErrMalformedResponse)

	f3 := &byteFrame{buf: []byte("hi\r\n")}
	dst := make([]byte, 2)
	noError(t, f3.readFramedValueInto(dst))
	equal(t, []byte("hi"), dst)

	f4 := &byteFrame{buf: []byte("hiXX")}
	isErrorIs(t, f4.readFramedValueInto(make([]byte, 2)), ErrMalformedResponse)
	equal(t, 0, (&byteFrame{}).buffered())
}

func TestReadCRLFAndOneByte(t *testing.T) {
	noError(t, readCRLF(bytes.NewReader([]byte("\r\n"))))
	isErrorIs(t, readCRLF(bytes.NewReader([]byte("\n\n"))), ErrMalformedResponse)
	err := readCRLF(bytes.NewReader([]byte("\r")))
	if err == nil {
		t.Fatal("expected short CRLF error")
	}

	b, err := readOneByte(bytes.NewReader([]byte("Z")))
	noError(t, err)
	equal(t, byte('Z'), b)

	dst := make([]byte, 2)
	noError(t, readFramedValueIntoFrom(bytes.NewReader([]byte("ab\r\n")), dst))
	equal(t, []byte("ab"), dst)
}
