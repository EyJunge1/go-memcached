package memcached

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

var responsePool = sync.Pool{
	New: func() any {
		return &response{
			endIndicator: endIndicatorNoReply,
			limitedLines: 0,
			specEndLine:  nil,
			rawLines:     nil,
			udpEnabled:   false,
		}
	},
}

// responseEndIndicator chooses how recv drains a reply.
type responseEndIndicator uint8

const (
	endIndicatorUnknown responseEndIndicator = iota
	// endIndicatorNoReply means the client must not wait for a reply.
	endIndicatorNoReply
	// endIndicatorLimitedLines reads a fixed number of '\n'-terminated records.
	endIndicatorLimitedLines
	// endIndicatorSpecificEndLine reads '\n' records until specEndLine.
	endIndicatorSpecificEndLine
	// endIndicatorQuietBarrier reads until MN and keeps quiet error replies.
	endIndicatorQuietBarrier
	// endIndicatorRetrieval reads VALUE/data pairs until END into items.
	endIndicatorRetrieval
)

// response is one decoded server frame.
type response struct {
	// endIndicator chooses how recv drains the connection buffer.
	endIndicator responseEndIndicator
	// limitedLines is how many protocol records read1 consumes. 0 means none.
	limitedLines uint8
	// specEndLine is the terminator read2 stops on.
	specEndLine []byte

	// rawLines are records split on '\n', including VALUE/VA bodies.
	rawLines [][]byte

	// udpEnabled is set by the client before send when the node is UDP.
	udpEnabled bool
	udpReqID   uint16

	// afterRecv parses the frame before the socket returns to the pool.
	afterRecv func(*response) error

	// items is filled by endIndicatorRetrieval.
	items   []Item
	withCAS bool

	maxValueBytes int
	readTimeout   time.Duration
	readCtx       context.Context
	readConn      wireConn
}

// buildNoReplyResponse returns a pooled response that waits for no reply.
func buildNoReplyResponse() *response {
	resp := responsePool.Get().(*response)
	resp.endIndicator = endIndicatorNoReply
	return resp
}

// buildLimitedLineResponse returns a pooled response that reads a fixed number of lines.
func buildLimitedLineResponse(lines uint8) *response {
	resp := responsePool.Get().(*response)
	resp.endIndicator = endIndicatorLimitedLines
	resp.limitedLines = lines
	resetRawLines(resp, int(lines))
	return resp
}

// buildSpecEndLineResponse returns a pooled response that reads until endLine.
func buildSpecEndLineResponse(endLine []byte, predictLines int) *response {
	if predictLines <= 0 {
		predictLines = 8
	}

	resp := responsePool.Get().(*response)
	resp.endIndicator = endIndicatorSpecificEndLine
	resp.specEndLine = endLine
	resetRawLines(resp, predictLines)
	return resp
}

const rawLinesKeepCap = 32

// resetRawLines reuses or reallocates rawLines with at least hint capacity.
func resetRawLines(resp *response, hint int) {
	if hint < 1 {
		hint = 1
	}
	if cap(resp.rawLines) >= hint {
		resp.rawLines = resp.rawLines[:0]
		return
	}
	resp.rawLines = make([][]byte, 0, hint)
}

// release clears the response and returns it to the pool.
func (resp *response) release() {
	resp.endIndicator = endIndicatorUnknown
	resp.limitedLines = 0
	resp.specEndLine = nil
	for i := range resp.rawLines {
		resp.rawLines[i] = nil
	}
	if cap(resp.rawLines) > rawLinesKeepCap {
		resp.rawLines = make([][]byte, 0, rawLinesKeepCap)
	} else {
		resp.rawLines = resp.rawLines[:0]
	}
	resp.udpEnabled = false
	resp.udpReqID = 0
	resp.afterRecv = nil
	resp.items = nil
	resp.withCAS = false
	resp.maxValueBytes = 0
	resp.readTimeout = 0
	resp.readCtx = nil
	resp.readConn = nil
	responsePool.Put(resp)
}

// buildRetrievalResponse returns a pooled response that parses VALUE pairs into items.
func buildRetrievalResponse(nKeys int, withCAS bool) *response {
	resp := responsePool.Get().(*response)
	resp.endIndicator = endIndicatorRetrieval
	resp.withCAS = withCAS
	if nKeys > 0 {
		resp.items = make([]Item, 0, nKeys)
	}
	return resp
}

// buildQuietBarrierResponse returns a response that reads until MN.
func buildQuietBarrierResponse() *response {
	resp := buildSpecEndLineResponse(_MetaMNCRLFBytes, 4)
	resp.endIndicator = endIndicatorQuietBarrier
	return resp
}

// appendMetaQuietBarrier appends mn [P] [L] after raw.
func appendMetaQuietBarrier(raw []byte, f MetaNoop) []byte {
	if f.P == "" && f.L == "" {
		return append(raw, _MnCRLFBytes...)
	}
	b := newProtocolBuilder().AddString("mn")
	defer b.release()
	b.AddFlagString("P", f.P)
	b.AddFlagString("L", f.L)
	line, box := b.AddCRLF().takeBytes()
	raw = append(raw, line...)
	putBuilderBox(box)
	return raw
}

// protocolReader is the line and framed-body reader used by recv.
type protocolReader interface {
	readLine(delim byte) ([]byte, error)
	readExact(n int) ([]byte, error)
}

// payloadReader is an in-memory protocolReader for tests.
type payloadReader struct {
	r *bufio.Reader
}

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
	buf := make([]byte, n)
	_, err := io.ReadFull(p.r, buf)
	return buf, err
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

type wireProtocolReader struct{ c wireConn }

func (w wireProtocolReader) readLine(delim byte) ([]byte, error) {
	return w.c.readLine(delim)
}

func (w wireProtocolReader) readExact(n int) ([]byte, error) {
	return w.c.readExact(n)
}

// recv reads the server frame according to endIndicator.
func (resp *response) recv(ctx context.Context, rr wireConn, readTimeout time.Duration) error {
	if has := selectProximateDeadline(ctx, rr, readTimeout, clock, true); has {
		defer clearReadDeadline(rr)
	}
	if ctx != nil && ctx.Done() != nil {
		stop := watchContextCancel(ctx, rr)
		defer stop()
	}

	if resp.udpEnabled {
		payload, err := assembleUDP(rr, resp.udpReqID, udpFragmentCap(resp.maxValueBytes))
		if err != nil {
			return err
		}
		return resp.recvFrom(newPayloadReader(payload))
	}
	if c, ok := rr.(*conn); ok {
		return resp.recvFrom(c)
	}
	return resp.recvFrom(wireProtocolReader{rr})
}

func (resp *response) recvFrom(reader protocolReader) error {
	switch resp.endIndicator {
	case endIndicatorNoReply:
		return nil
	case endIndicatorLimitedLines:
		return resp.read1(reader)
	case endIndicatorSpecificEndLine:
		return resp.read2(reader, false)
	case endIndicatorQuietBarrier:
		return resp.read2(reader, true)
	case endIndicatorRetrieval:
		return resp.readRetrieval(reader)
	default:
	}

	return fmt.Errorf("unknown end indicator: %w", ErrMalformedResponse)
}

// selectProximateDeadline applies the sooner of ctx and timeout to the conn.
func selectProximateDeadline(
	ctx context.Context, rr wireConn, timeout time.Duration, clock clockFunc, isRead bool,
) (ok bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	if timeout < 0 {
		timeout = 0
	}

	var (
		deadline time.Time
		has      bool
	)
	if timeout > 0 {
		deadline = clock().Add(timeout)
		has = true
	}

	if ctxDeadline, ok := ctx.Deadline(); ok {
		if !has || ctxDeadline.Before(deadline) {
			deadline = ctxDeadline
			has = true
		}
	}

	if has {
		if isRead {
			_ = rr.setReadDeadline(deadline)
		} else {
			_ = rr.setWriteDeadline(deadline)
		}
	}

	return has
}

// protocolDataBlockSize parses the VALUE or VA payload size, if present.
func protocolDataBlockSize(line []byte) (size uint64, has bool, err error) {
	trimmed := trimCRLF(line)
	switch {
	case bytes.HasPrefix(trimmed, _ValueSpaceBytes):
		size, err = parseValueDataLen(trimmed)
		return size, true, err
	case bytes.HasPrefix(trimmed, _VASpaceBytes):
		rest := trimmed[3:]
		end := bytes.IndexByte(rest, ' ')
		if end < 0 {
			end = len(rest)
		}
		size, err = parseUintFromBytes(rest[:end])
		if err != nil {
			return 0, true, fmt.Errorf("invalid VA size: %w", ErrMalformedResponse)
		}
		return size, true, nil
	default:
		return 0, false, nil
	}
}

// appendDataBlock appends a header and its length-framed body, if any.
func (resp *response) appendDataBlock(rr protocolReader, header []byte) (consumed int, err error) {
	size, has, err := protocolDataBlockSize(header)
	if err != nil {
		return 0, fmt.Errorf("data block size: %w", err)
	}
	if !has {
		resp.rawLines = append(resp.rawLines, header)
		return 1, nil
	}
	limit := resp.maxValueBytes
	if limit <= 0 {
		limit = defaultMaxValueBytes
	}
	if size > uint64(limit) {
		return 0, fmt.Errorf("data block too large: %w", ErrMalformedResponse)
	}
	payload, err := rr.readExact(int(size) + 2) //nolint:gosec // G115: size already bounded by maxValueBytes
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, fmt.Errorf("short data block: %w", ErrMalformedResponse)
		}
		return 0, fmt.Errorf("read data block: %w", err)
	}
	if !bytes.HasSuffix(payload, _CRLFBytes) {
		return 0, fmt.Errorf("data block missing CRLF: %w", ErrMalformedResponse)
	}
	if uint64(len(payload)) != size+2 {
		return 0, fmt.Errorf("data block length mismatch: %w", ErrMalformedResponse)
	}
	resp.rawLines = append(resp.rawLines, header)
	resp.rawLines = append(resp.rawLines, payload)
	refreshReadDeadlineIfIdle(resp, rr)
	return 2, nil
}

// read1 reads limitedLines protocol records. VALUE/VA bodies are length-framed
// and count as the next record.
func (resp *response) read1(rr protocolReader) error {
	read := 0
	sr, sliceOK := rr.(sliceReader)
	for read < int(resp.limitedLines) {
		var line []byte
		var err error
		if sliceOK {
			line, err = sr.readSlice('\n')
		} else {
			line, err = rr.readLine('\n')
		}
		if err != nil {
			return fmt.Errorf("dispatchRequest read: %w", err)
		}

		if err = forecastCommonFaultLine(line); err != nil {
			return err
		}
		if sliceOK {
			if _, has, sizeErr := protocolDataBlockSize(line); sizeErr == nil && has {
				line = bytes.Clone(line)
			}
		}

		consumed, err := resp.appendDataBlock(rr, line)
		if err != nil {
			return err
		}
		read += consumed
	}

	return nil
}

// read2 reads until specEndLine. Bodies are length-framed.
// collectFaults keeps EX/NF/NS/CLIENT_ERROR until MN.
func (resp *response) read2(rr protocolReader, collectFaults bool) error {
	for {
		line, err := rr.readLine('\n')
		if err != nil {
			return fmt.Errorf("dispatchRequest read: %w", err)
		}

		if bytes.Equal(trimCRLF(line), trimCRLF(resp.specEndLine)) {
			resp.rawLines = append(resp.rawLines, line)
			break
		}

		if err = forecastCommonFaultLine(line); err != nil {
			if !collectFaults || errors.Is(err, ErrServerError) {
				return err
			}
		}

		if _, err = resp.appendDataBlock(rr, line); err != nil {
			return err
		}
	}

	return nil
}

// sliceReader can return a line that is only valid until the next read.
type sliceReader interface {
	readSlice(delim byte) ([]byte, error)
}

type framedValueIntoReader interface {
	readFramedValueInto(dst []byte) error
}

// Combining a small key and value saves one allocation without crossing into
// a larger allocator size class than the two separate buffers would occupy.
const maxCombinedRetrievalBytes = 64

func readFramedValueIntoFrom(r io.Reader, dst []byte) error {
	if _, err := io.ReadFull(r, dst); err != nil {
		return err
	}
	var crlf [2]byte
	if _, err := io.ReadFull(r, crlf[:]); err != nil {
		return err
	}
	if crlf[0] != '\r' || crlf[1] != '\n' {
		return fmt.Errorf("data block missing CRLF: %w", ErrMalformedResponse)
	}
	return nil
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

// readRetrieval reads VALUE/data pairs until END without keeping rawLines.
func (resp *response) readRetrieval(rr protocolReader) error {
	items := resp.items[:0]
	limit := resp.maxValueBytes
	if limit <= 0 {
		limit = defaultMaxValueBytes
	}
	sr, sliceOK := rr.(sliceReader)

	for {
		var line []byte
		var err error
		if sliceOK {
			line, err = sr.readSlice('\n')
		} else {
			line, err = rr.readLine('\n')
		}
		if err != nil {
			return fmt.Errorf("dispatchRequest read: %w", err)
		}
		trimmed := trimCRLF(line)
		if bytes.Equal(trimmed, _EndBytes) {
			resp.items = items
			return nil
		}
		if !bytes.HasPrefix(trimmed, _ValueSpaceBytes) {
			if err = forecastCommonFaultLine(line); err != nil {
				return err
			}
			return fmt.Errorf("unexpected retrieval line: %w", ErrMalformedResponse)
		}

		var item Item
		dataLen, err := parseValueLineBorrowed(trimmed, &item, resp.withCAS)
		if err != nil {
			return err
		}
		if dataLen > uint64(limit) {
			return fmt.Errorf("data block too large: %w", ErrMalformedResponse)
		}
		keyLen, valueLen := len(item.Key), int(dataLen) //nolint:gosec // G115: dataLen is bounded above
		if keyLen+valueLen <= maxCombinedRetrievalBytes {
			owned := make([]byte, keyLen+valueLen)
			copy(owned, item.Key)
			item.Key = owned[:keyLen:keyLen]
			item.Value = owned[keyLen:]
		} else {
			item.Key = bytes.Clone(item.Key)
			item.Value = make([]byte, valueLen)
		}
		err = readFramedValueInto(rr, item.Value)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return fmt.Errorf("short data block: %w", ErrMalformedResponse)
			}
			return fmt.Errorf("read data block: %w", err)
		}
		items = append(items, item)
		refreshReadDeadlineIfIdle(resp, rr)
	}
}

// mapStatusLine accepts want, else a known protocol error, else ErrMalformedResponse.
func mapStatusLine(resp *response, want []byte) error {
	if resp.endIndicator == endIndicatorNoReply {
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

func refreshReadDeadlineIfIdle(resp *response, rr protocolReader) {
	if resp == nil || resp.readConn == nil {
		return
	}
	if n := readerBuffered(rr); n > 0 {
		return
	}
	_ = selectProximateDeadline(resp.readCtx, resp.readConn, resp.readTimeout, clock, true)
}
