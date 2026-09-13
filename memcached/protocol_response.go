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

type responseEndIndicator uint8

const (
	endIndicatorUnknown responseEndIndicator = iota
	// endIndicatorNoReply indicates the response is no reply
	// and the client should not wait for the response.
	endIndicatorNoReply
	// endIndicatorLimitedLines indicates the response is limited lines;
	// the client should read line from response with limited lines with delimiter '\n'.
	endIndicatorLimitedLines
	// endIndicatorSpecificEndLine indicates the response is specific end line;
	// the client should read lines from response until the specific end line.
	// The delimiter is '\n'.
	endIndicatorSpecificEndLine
	// endIndicatorQuietBarrier reads until MN, collecting quiet (non-nominal) replies.
	endIndicatorQuietBarrier
)

// response represents a structural response from memcached server.
type response struct {
	// endIndicator indicates the parser how to read the whole bytes from the
	// connection receiving buffer.
	endIndicator responseEndIndicator
	// limitedLines is the number of lines to read from the connection.
	// If limitedLines equals 0, it means the response is not ready to be read
	// from the connection. 1 means the response is
	// ready to be read from the connection.
	limitedLines uint8
	// specEndLine is the specific end line of the response, it helps to read
	// from the connection.
	specEndLine []byte

	// rawLines is the raw bytes of the response, it has been divided by '\n'.
	// .e.g. "VALUE key 0 5\r\nvalue\r\nEND\r\n" will be divided into
	// ["VALUE key 0 5\r\n", "value\r\n", "END\r\n"].
	rawLines [][]byte

	// This field is used to indicate whether the request is UDP enabled.
	// And it's set by the memcached client before sending the request.
	udpEnabled bool
	udpReqID   uint16

	// afterRecv runs after a successful frame read and before the conn is
	// returned to the pool, so parse/expect failures can discard a desynced conn.
	afterRecv func(*response) error

	maxValueBytes int
	readTimeout   time.Duration
	readCtx       context.Context
	readConn      memcachedConn
}

func buildNoReplyResponse() *response {
	resp := responsePool.Get().(*response)
	resp.endIndicator = endIndicatorNoReply
	return resp
}

func buildLimitedLineResponse(lines uint8) *response {
	resp := responsePool.Get().(*response)
	resp.endIndicator = endIndicatorLimitedLines
	resp.limitedLines = lines
	resetRawLines(resp, int(lines))
	return resp
}

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
	resp.maxValueBytes = 0
	resp.readTimeout = 0
	resp.readCtx = nil
	resp.readConn = nil
	responsePool.Put(resp)
}

func buildQuietBarrierResponse() *response {
	resp := buildSpecEndLineResponse(_MetaMNCRLFBytes, 4)
	resp.endIndicator = endIndicatorQuietBarrier
	return resp
}

func appendMetaQuietBarrier(raw []byte) []byte {
	out := make([]byte, 0, len(raw)+4)
	out = append(out, raw...)
	out = append(out, []byte("mn\r\n")...)
	return out
}

type protocolReader interface {
	readLine(delim byte) ([]byte, error)
	readExact(n int) ([]byte, error)
}

type payloadReader struct {
	r *bufio.Reader
}

func newPayloadReader(payload []byte) *payloadReader {
	return &payloadReader{r: bufio.NewReader(bytes.NewReader(payload))}
}

func (p *payloadReader) readLine(delim byte) ([]byte, error) {
	return p.r.ReadBytes(delim)
}

func (p *payloadReader) readExact(n int) ([]byte, error) {
	buf := make([]byte, n)
	_, err := io.ReadFull(p.r, buf)
	return buf, err
}

func (resp *response) recv(ctx context.Context, rr memcachedConn, readTimeout time.Duration) error {
	if has := selectProximateDeadline(ctx, rr, readTimeout, nowFunc, true); has {
		defer func() { _ = rr.setReadDeadline(zeroTime) }()
	}
	stop := watchContextCancel(ctx, rr)
	defer stop()

	var reader protocolReader = rr
	if resp.udpEnabled {
		payload, err := assembleUDP(rr, resp.udpReqID)
		if err != nil {
			return err
		}
		reader = newPayloadReader(payload)
	}

	switch resp.endIndicator {
	case endIndicatorNoReply:
		return nil
	case endIndicatorLimitedLines:
		return resp.read1(reader)
	case endIndicatorSpecificEndLine:
		return resp.read2(reader, false)
	case endIndicatorQuietBarrier:
		return resp.read2(reader, true)
	default:
	}

	return ErrUnknownIndicator
}

func selectProximateDeadline(
	ctx context.Context, rr memcachedConn, timeout time.Duration, nowFunc nowFuncType, isRead bool,
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
		deadline = nowFunc().Add(timeout)
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

func protocolDataBlockSize(line []byte) (size uint64, has bool, err error) {
	trimmed := trimCRLF(line)
	switch {
	case bytes.HasPrefix(trimmed, []byte("VALUE ")):
		item := &Item{}
		// Framing only needs <bytes>; CAS is optional on the wire (get vs gets).
		size, err = parseValueLine(trimmed, item, false)
		return size, true, err
	case bytes.HasPrefix(trimmed, []byte("VA ")):
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
	payload, err := rr.readExact(int(size) + 2)
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
	if resp.readConn != nil {
		_ = selectProximateDeadline(resp.readCtx, resp.readConn, resp.readTimeout, nowFunc, true)
	}
	return 2, nil
}

// read1 reads the response from the connection with limited protocol lines.
// VALUE/VA payloads are length-framed and count as the following line.
func (resp *response) read1(rr protocolReader) error {
	read := 0
	for read < int(resp.limitedLines) {
		line, err := rr.readLine('\n')
		if err != nil {
			return fmt.Errorf("dispatchRequest read: %w", err)
		}

		if err = forecastCommonFaultLine(line); err != nil {
			return err
		}

		consumed, err := resp.appendDataBlock(rr, line)
		if err != nil {
			return err
		}
		read += consumed
	}

	return nil
}

// read2 reads protocol lines until specEndLine. Data blocks are length-framed.
// When collectFaults is set, EX/NF/NS/CLIENT_ERROR stay on the wire until MN.
func (resp *response) read2(rr protocolReader, collectFaults bool) error {
	for {
		line, err := rr.readLine('\n')
		if err != nil {
			return fmt.Errorf("dispatchRequest read: %w", err)
		}

		if bytes.Equal(line, resp.specEndLine) {
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

// mapStatusLine accepts want, otherwise protocol sentinels from forecast, else malformed.
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

func expectLine(want []byte) func(*response) error {
	return func(resp *response) error {
		return mapStatusLine(resp, want)
	}
}
