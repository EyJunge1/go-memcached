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
	// endIndicatorRetrieval reads VALUE/data pairs until END into getDst.
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

	// getDst is filled by endIndicatorRetrieval. getKeys are the wired keys
	// for this node; getIdx maps local key i onto getDst (nil = identity).
	getDst  []GetResult
	getKeys [][]byte
	getIdx  []int
	withCAS bool

	// metaItem is filled in-place for a single Meta().Get when set.
	metaItem   *MetaItem
	metaFilled bool

	// metaPipe* stream Meta().Pipeline replies into dest by opaque (no VA clone).
	metaPipeC        *Client
	metaPipeDst      []MetaResult
	metaPipeGroup    []preparedMetaOp
	metaPipeByOpaque map[string]int
	metaSawReply     bool

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
	resp.getDst = nil
	resp.getKeys = nil
	resp.getIdx = nil
	resp.withCAS = false
	resp.metaItem = nil
	resp.metaFilled = false
	resp.metaPipeC = nil
	resp.metaPipeDst = nil
	resp.metaPipeGroup = nil
	resp.metaPipeByOpaque = nil
	resp.metaSawReply = false
	resp.maxValueBytes = 0
	resp.readTimeout = 0
	resp.readCtx = nil
	resp.readConn = nil
	responsePool.Put(resp)
}

// buildRetrievalResponse returns a pooled response that parses VALUE pairs into dst.
func buildRetrievalResponse(keys [][]byte, origIdx []int, dst []GetResult, withCAS bool) *response {
	resp := responsePool.Get().(*response)
	resp.endIndicator = endIndicatorRetrieval
	resp.withCAS = withCAS
	resp.getKeys = keys
	resp.getIdx = origIdx
	resp.getDst = dst
	return resp
}

func takeValueBuffer(dst []byte, n int) []byte {
	if n <= 0 {
		if dst != nil {
			return dst[:0]
		}
		return nil
	}
	if cap(dst) >= n {
		return dst[:n]
	}
	return make([]byte, n)
}

func sameByteArray(a, b []byte) bool {
	if cap(a) == 0 || cap(b) == 0 {
		return false
	}
	return &a[:cap(a)][0] == &b[:cap(b)][0]
}

func ownValue(dst, src []byte) []byte {
	if len(src) == 0 {
		if dst != nil {
			return dst[:0]
		}
		return src
	}
	if cap(dst) >= len(src) {
		if sameByteArray(dst, src) {
			return src
		}
		dst = dst[:len(src)]
		copy(dst, src)
		return dst
	}
	return src
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

func readProtocolLine(rr protocolReader, sr sliceReader, sliceOK bool) ([]byte, error) {
	if sliceOK {
		return sr.readSlice('\n')
	}
	return rr.readLine('\n')
}

// read1 reads limitedLines protocol records. VALUE/VA bodies are length-framed
// and count as the next record.
func (resp *response) read1(rr protocolReader) error {
	sr, sliceOK := rr.(sliceReader)
	if resp.metaItem != nil {
		line, err := readProtocolLine(rr, sr, sliceOK)
		if err != nil {
			return fmt.Errorf("dispatchRequest read: %w", err)
		}
		if err = forecastCommonFaultLine(line); err != nil {
			return err
		}
		if err = resp.fillMetaItem(rr, line); err != nil {
			return err
		}
		resp.metaFilled = true
		return nil
	}

	read := 0
	for read < int(resp.limitedLines) {
		line, err := readProtocolLine(rr, sr, sliceOK)
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
	sr, sliceOK := rr.(sliceReader)
	metaFilled := false
	var charge error
	for {
		line, err := readProtocolLine(rr, sr, sliceOK)
		if err != nil {
			return fmt.Errorf("dispatchRequest read: %w", err)
		}

		if bytes.Equal(trimCRLF(line), trimCRLF(resp.specEndLine)) {
			if resp.metaItem == nil && resp.metaPipeDst == nil {
				if sliceOK {
					line = bytes.Clone(line)
				}
				resp.rawLines = append(resp.rawLines, line)
			}
			break
		}

		if err = forecastCommonFaultLine(line); err != nil {
			if !collectFaults || errors.Is(err, ErrServerError) {
				return err
			}
			if charge == nil {
				charge = err
			}
			if resp.metaItem != nil || resp.metaPipeDst != nil {
				continue
			}
		} else if resp.metaPipeDst != nil {
			if !isMetaNoopLine(line) {
				if err = resp.fillMetaPipe(rr, line); err != nil {
					return err
				}
			}
			continue
		} else if resp.metaItem != nil && !metaFilled && !isMetaNoopLine(line) {
			if err = resp.fillMetaItem(rr, line); err != nil {
				return err
			}
			metaFilled = true
			resp.metaFilled = true
			continue
		}

		if resp.metaItem != nil {
			continue
		}
		if sliceOK {
			line = bytes.Clone(line)
		}
		if _, err = resp.appendDataBlock(rr, line); err != nil {
			return err
		}
	}

	return charge
}

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
	limit := resp.maxValueBytes
	if limit <= 0 {
		limit = defaultMaxValueBytes
	}
	if size > uint64(limit) {
		return fmt.Errorf("data block too large: %w", ErrMalformedResponse)
	}
	n := int(size) //nolint:gosec // G115: size is bounded above
	item.Value = takeValueBuffer(item.Value, n)
	if err = readFramedValueInto(rr, item.Value); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return fmt.Errorf("short data block: %w", ErrMalformedResponse)
		}
		return fmt.Errorf("read data block: %w", err)
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

func discardFramedValue(rr protocolReader, n int) error {
	if n < 0 {
		return fmt.Errorf("negative read: %w", ErrMalformedResponse)
	}
	switch r := rr.(type) {
	case *conn:
		return discardFramedValueFrom(r.rr, n)
	case *payloadReader:
		return discardFramedValueFrom(r.r, n)
	default:
		buf := make([]byte, n)
		return readFramedValueInto(rr, buf)
	}
}

func discardFramedValueFrom(r io.Reader, n int) error {
	var buf [256]byte
	left := n
	for left > 0 {
		chunk := buf[:]
		if left < len(chunk) {
			chunk = buf[:left]
		}
		if _, err := io.ReadFull(r, chunk); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return fmt.Errorf("short data block: %w", ErrMalformedResponse)
			}
			return fmt.Errorf("read data block: %w", err)
		}
		left -= len(chunk)
	}
	var crlf [2]byte
	if _, err := io.ReadFull(r, crlf[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return fmt.Errorf("short data block: %w", ErrMalformedResponse)
		}
		return fmt.Errorf("read data block: %w", err)
	}
	if crlf[0] != '\r' || crlf[1] != '\n' {
		return fmt.Errorf("data block missing CRLF: %w", ErrMalformedResponse)
	}
	return nil
}

// readRetrieval reads VALUE/data pairs until END into getDst.
func (resp *response) readRetrieval(rr protocolReader) error {
	limit := resp.maxValueBytes
	if limit <= 0 {
		limit = defaultMaxValueBytes
	}
	sr, sliceOK := rr.(sliceReader)
	cursor := 0

	for {
		line, err := readProtocolLine(rr, sr, sliceOK)
		if err != nil {
			return fmt.Errorf("dispatchRequest read: %w", err)
		}
		trimmed := trimCRLF(line)
		if bytes.Equal(trimmed, _EndBytes) {
			return nil
		}
		if !bytes.HasPrefix(trimmed, _ValueSpaceBytes) {
			if err = forecastCommonFaultLine(line); err != nil {
				return err
			}
			return fmt.Errorf("unexpected retrieval line: %w", ErrMalformedResponse)
		}

		var parsed Item
		dataLen, err := parseValueLineBorrowed(trimmed, &parsed, resp.withCAS)
		if err != nil {
			return err
		}
		if dataLen > uint64(limit) {
			return fmt.Errorf("data block too large: %w", ErrMalformedResponse)
		}
		local, ok := matchRetrievalKey(resp.getKeys, parsed.Key, &cursor)
		if !ok {
			return fmt.Errorf("unexpected VALUE key: %w", ErrMalformedResponse)
		}
		dstIdx := local
		if resp.getIdx != nil {
			if local < 0 || local >= len(resp.getIdx) {
				return fmt.Errorf("unexpected VALUE key: %w", ErrMalformedResponse)
			}
			dstIdx = resp.getIdx[local]
		}
		if dstIdx < 0 || dstIdx >= len(resp.getDst) {
			return fmt.Errorf("unexpected VALUE key: %w", ErrMalformedResponse)
		}

		valueLen := int(dataLen) //nolint:gosec // G115: dataLen is bounded above
		slot := &resp.getDst[dstIdx]
		slot.Found = true
		slot.Flags = parsed.Flags
		slot.CAS = parsed.CAS
		slot.Value = takeValueBuffer(slot.Value, valueLen)
		err = readFramedValueInto(rr, slot.Value)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return fmt.Errorf("short data block: %w", ErrMalformedResponse)
			}
			return fmt.Errorf("read data block: %w", err)
		}
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
