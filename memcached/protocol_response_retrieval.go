package memcached

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"time"
)

func (resp *response) fillMetaItem(rr protocolReader, header []byte) error {
	item := resp.metaItem
	if item == nil {
		return fmt.Errorf("missing meta dest: %w", ErrMalformedResponse)
	}
	_, size, hasSize, err := parseMetaHeader(header, item)
	if err != nil {
		return err
	}
	if item.status != MetaStatusVA {
		return nil
	}
	if !hasSize {
		return fmt.Errorf("missing value size: %w", ErrMalformedResponse)
	}
	n, err := cappedValueSize(size, resp.maxValueBytes)
	if err != nil {
		return err
	}
	item.Value = takeValueBuffer(item.Value, n)
	if err = readFramedValueInto(rr, item.Value); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return fmt.Errorf("short data block: %w", ErrMalformedResponse)
		}
		return fmt.Errorf("read data block: %w", err)
	}
	return nil
}

// readRetrieval reads VALUE/data pairs until END into getDst.
func (resp *response) readRetrieval(rr protocolReader) error {
	sr, sliceOK := rr.(sliceReader)
	cursor := 0

	for {
		line, err := readProtocolLine(rr, sr, sliceOK)
		if err != nil {
			resp.invalidateRetrievalDest()
			return fmt.Errorf("dispatchRequest read: %w", err)
		}
		trimmed := trimCRLF(line)
		if bytes.Equal(trimmed, _EndBytes) {
			return nil
		}
		if !bytes.HasPrefix(trimmed, _ValueSpaceBytes) {
			if err = forecastCommonFaultLine(line); err != nil {
				resp.invalidateRetrievalDest()
				return err
			}
			resp.invalidateRetrievalDest()
			return fmt.Errorf("unexpected retrieval line: %w", ErrMalformedResponse)
		}

		var parsed Item
		dataLen, err := parseValueLineBorrowed(trimmed, &parsed, resp.withCAS)
		if err != nil {
			resp.invalidateRetrievalDest()
			return err
		}
		valueLen, err := cappedValueSize(dataLen, resp.maxValueBytes)
		if err != nil {
			resp.invalidateRetrievalDest()
			return err
		}
		slot, err := resp.retrievalSlot(parsed.Key, &cursor)
		if err != nil {
			resp.invalidateRetrievalDest()
			return err
		}
		if slot.Found {
			resp.invalidateRetrievalDest()
			return fmt.Errorf("duplicate VALUE for key: %w", ErrMalformedResponse)
		}

		slot.Found = true
		slot.Flags = parsed.Flags
		slot.CAS = parsed.CAS
		slot.Value = takeValueBuffer(slot.Value, valueLen)
		err = readFramedValueInto(rr, slot.Value)
		if err != nil {
			resp.invalidateRetrievalDest()
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return fmt.Errorf("short data block: %w", ErrMalformedResponse)
			}
			return fmt.Errorf("read data block: %w", err)
		}
		refreshReadDeadlineIfIdle(resp, rr)
	}
}

// invalidateRetrievalDest clears slots written by this retrieval wave so a
// caller does not observe partial fills after a parse or transport error.
// Other node groups sharing getDst stay untouched when getIdx is set.
func (resp *response) invalidateRetrievalDest() {
	clearSlot := func(slot *GetResult) {
		if slot == nil {
			return
		}
		val := slot.Value
		*slot = GetResult{}
		if val != nil {
			slot.Value = val[:0]
		}
	}
	if resp.getOneDst != nil {
		clearSlot(resp.getOneDst)
		return
	}
	if resp.getIdx != nil {
		for _, dstIdx := range resp.getIdx {
			if dstIdx >= 0 && dstIdx < len(resp.getDst) {
				clearSlot(&resp.getDst[dstIdx])
			}
		}
		return
	}
	for i := range resp.getDst {
		clearSlot(&resp.getDst[i])
	}
}

func (resp *response) retrievalSlot(want []byte, cursor *int) (*GetResult, error) {
	if resp.getOneDst != nil {
		if !bytes.Equal(resp.getOneKey, want) {
			return nil, fmt.Errorf("unexpected VALUE key: %w", ErrMalformedResponse)
		}
		return resp.getOneDst, nil
	}
	local, ok := matchRetrievalKey(resp.getKeys, want, cursor)
	if !ok {
		return nil, fmt.Errorf("unexpected VALUE key: %w", ErrMalformedResponse)
	}
	dstIdx := local
	if resp.getIdx != nil {
		if local < 0 || local >= len(resp.getIdx) {
			return nil, fmt.Errorf("unexpected VALUE key: %w", ErrMalformedResponse)
		}
		dstIdx = resp.getIdx[local]
	}
	if dstIdx < 0 || dstIdx >= len(resp.getDst) {
		return nil, fmt.Errorf("unexpected VALUE key: %w", ErrMalformedResponse)
	}
	return &resp.getDst[dstIdx], nil
}

// sliceReader can return a line that is only valid until the next read.
type sliceReader interface {
	readSlice(delim byte) ([]byte, error)
}

type framedValueIntoReader interface {
	readFramedValueInto(dst []byte) error
}

func readFramedValueIntoFrom(r io.Reader, dst []byte) error {
	if br, ok := r.(*bufio.Reader); ok {
		return readFramedValueIntoBufio(br, dst)
	}
	if _, err := io.ReadFull(r, dst); err != nil {
		return err
	}
	return readCRLF(r)
}

func readFramedValueIntoBufio(r *bufio.Reader, dst []byte) error {
	for len(dst) > 0 {
		n, err := r.Read(dst)
		dst = dst[n:]
		if err != nil {
			if errors.Is(err, io.EOF) && len(dst) > 0 {
				return io.ErrUnexpectedEOF
			}
			return err
		}
	}
	b0, err := r.ReadByte()
	if err != nil {
		return err
	}
	b1, err := r.ReadByte()
	if err != nil {
		return err
	}
	if b0 != '\r' || b1 != '\n' {
		return fmt.Errorf("data block missing CRLF: %w", ErrMalformedResponse)
	}
	return nil
}

func readCRLF(r io.Reader) error {
	b0, err := readOneByte(r)
	if err != nil {
		return err
	}
	b1, err := readOneByte(r)
	if err != nil {
		return err
	}
	if b0 != '\r' || b1 != '\n' {
		return fmt.Errorf("data block missing CRLF: %w", ErrMalformedResponse)
	}
	return nil
}

func readOneByte(r io.Reader) (byte, error) {
	if br, ok := r.(*bufio.Reader); ok {
		return br.ReadByte()
	}
	var b [1]byte
	_, err := io.ReadFull(r, b[:])
	return b[0], err
}

func readFramedValueInto(rr protocolReader, dst []byte) error {
	if fr, ok := rr.(framedValueIntoReader); ok {
		return fr.readFramedValueInto(dst)
	}
	payload, err := rr.readExact(len(dst) + 2)
	if err != nil {
		return err
	}
	value, err := dataBlockValue(payload, uint64(len(dst)))
	if err != nil {
		return err
	}
	copy(dst, value)
	return nil
}

// mapStatusLine accepts want, else a known protocol error, else ErrMalformedResponse.
func mapStatusLine(resp *response, want []byte) error {
	if resp.endIndicator == _endIndicatorNoReply {
		return nil
	}
	if n := len(resp.rawLines); n != 1 {
		return fmt.Errorf("expect only 1 line, but got %d: %w", n, ErrMalformedResponse)
	}

	line := resp.rawLines[0]
	if bytes.Equal(line, want) || bytes.Equal(trimCRLF(line), trimCRLF(want)) {
		return nil
	}
	if err := forecastCommonFaultLine(line); err != nil {
		return err
	}

	snippet := line
	if len(snippet) > 256 {
		snippet = snippet[:256]
	}
	return fmt.Errorf("unexpected response: %s: %w", snippet, ErrMalformedResponse)
}

func mapDeletedReply(resp *response) error {
	return mapStatusLine(resp, _DeletedCRLFBytes)
}

func mapTouchedReply(resp *response) error {
	return mapStatusLine(resp, _TouchedCRLFBytes)
}

func readerBuffered(rr protocolReader) int {
	type withBuf interface{ buffered() int }
	if b, ok := rr.(withBuf); ok {
		return b.buffered()
	}
	return -1
}

// ensureNoTrailingBytes reports ErrMalformedResponse when unread reply bytes
// remain after a complete response wave. n < 0 means the reader cannot report
// buffered size and the check is skipped.
func ensureNoTrailingBytes(rr any) error {
	type withBuf interface{ buffered() int }
	n := -1
	switch v := rr.(type) {
	case withBuf:
		n = v.buffered()
	case protocolReader:
		n = readerBuffered(v)
	}
	if n > 0 {
		return fmt.Errorf("trailing bytes after response: %w", ErrMalformedResponse)
	}
	return nil
}

func refreshReadDeadlineIfIdle(resp *response, rr protocolReader) {
	if resp == nil || resp.readConn == nil {
		return
	}
	if n := readerBuffered(rr); n > 0 {
		return
	}
	_ = selectProximateDeadline(resp.readCtx, resp.readConn, resp.readTimeout, time.Now, true /* isRead */)
}
