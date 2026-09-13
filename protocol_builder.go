package memcached

import (
	"bufio"
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxKeySize   = 250
	maxValueSize = math.MaxUint32
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
	b.buf.WriteString(strconv.Itoa(i))
	b.buf.WriteByte(_SpaceByte)
	return b
}

func (b *protocolBuilder) AddUint(i uint64) *protocolBuilder {
	b.buf.WriteString(strconv.FormatUint(i, 10))
	b.buf.WriteByte(_SpaceByte)
	return b
}

func (b *protocolBuilder) AddInt64(i int64) *protocolBuilder {
	b.buf.WriteString(strconv.FormatInt(i, 10))
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
	b.buf.WriteString(strconv.FormatUint(tok, 10))
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

var requestPool = sync.Pool{
	New: func() any {
		return &request{
			cmd:        nil,
			key:        nil,
			raw:        nil,
			udpEnabled: false,
		}
	},
}

type request struct {
	cmd []byte // command name
	key []byte // key is nil if the command DOES NOT need key
	raw []byte

	// this field is used to indicate whether the request is UDP enabled.
	// And it's set by the memcached client before sending the request.
	udpEnabled bool
	udpReqID   uint16
}

func buildRequest(cmd []byte, key []byte, raw []byte) *request {
	req := requestPool.Get().(*request)
	req.cmd = cmd
	req.key = key
	req.raw = raw
	return req
}

func (req *request) release() {
	req.cmd = nil
	req.key = nil
	req.raw = nil
	req.udpEnabled = false
	req.udpReqID = 0

	requestPool.Put(req)
}

func (req *request) send(ctx context.Context, rr memcachedConn, writeTimeout time.Duration) (err error) {
	if has := selectProximateDeadline(ctx, rr, writeTimeout, nowFunc, false); has {
		defer func() { _ = rr.setWriteDeadline(zeroTime) }()
	}
	stop := watchContextCancel(ctx, rr)
	defer stop()

	if req.udpEnabled {
		return req.sendUDP(rr)
	}

	_, err = rr.Write(req.raw)
	return err
}

func watchContextCancel(ctx context.Context, rr memcachedConn) (stop func()) {
	if ctx == nil {
		return func() {}
	}
	cancelWatch := context.AfterFunc(ctx, func() {
		now := nowFunc()
		_ = rr.setReadDeadline(now)
		_ = rr.setWriteDeadline(now)
	})
	return func() { _ = cancelWatch() }
}

const udpHeaderSize = 8

var udpRequestID atomic.Uint32

func init() {
	seedUDPRequestID()
}

func seedUDPRequestID() {
	var b [2]byte
	if _, err := crand.Read(b[:]); err != nil {
		return
	}
	udpRequestID.Store(uint32(binary.BigEndian.Uint16(b[:])))
}

func nextUDPRequestID() uint16 {
	// 16-bit field; skip 0 so an unset/spoofed header never matches a live request.
	for {
		id := uint16(udpRequestID.Add(1) & 0xffff) //nolint:gosec // G115: protocol field is 16-bit
		if id != 0 {
			return id
		}
	}
}

func isUDPNetwork(network string) bool {
	return network == "udp" || network == "udp4" || network == "udp6"
}

// sendUDP sends the request to the memcached server.
// https://github.com/memcached/memcached/blob/master/doc/protocol.txt#L1875-L1914
func (req *request) sendUDP(rr memcachedConn) (err error) {
	header := make([]byte, udpHeaderSize)
	binary.BigEndian.PutUint16(header[0:2], req.udpReqID)
	binary.BigEndian.PutUint16(header[2:4], 0)
	binary.BigEndian.PutUint16(header[4:6], 1)
	datagram := make([]byte, 0, udpHeaderSize+len(req.raw))
	datagram = append(datagram, header...)
	datagram = append(datagram, req.raw...)

	_, err = rr.Write(datagram)
	return err
}

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
	resp.rawLines = make([][]byte, 0, lines)
	return resp
}

func buildSpecEndLineResponse(endLine []byte, predictLines int) *response {
	if predictLines <= 0 {
		predictLines = 8
	}

	resp := responsePool.Get().(*response)
	resp.endIndicator = endIndicatorSpecificEndLine
	resp.specEndLine = endLine
	resp.rawLines = make([][]byte, 0, predictLines)
	return resp
}

func (resp *response) release() {
	resp.endIndicator = endIndicatorUnknown
	resp.limitedLines = 0
	resp.specEndLine = nil
	resp.rawLines = nil
	resp.udpEnabled = false
	resp.udpReqID = 0
	resp.afterRecv = nil
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

func assembleUDP(rr memcachedConn, wantID uint16) ([]byte, error) {
	var slots [][]byte
	var total int
	got := 0
	for {
		dgram, err := rr.readDatagram()
		if err != nil {
			return nil, fmt.Errorf("udp datagram: %w", err)
		}
		if len(dgram) < udpHeaderSize {
			return nil, fmt.Errorf("short udp header: %w", ErrMalformedResponse)
		}
		id := binary.BigEndian.Uint16(dgram[0:2])
		if id != wantID {
			continue
		}
		seq := int(binary.BigEndian.Uint16(dgram[2:4]))
		tot := int(binary.BigEndian.Uint16(dgram[4:6]))
		if tot <= 0 {
			return nil, fmt.Errorf("invalid udp total: %w", ErrMalformedResponse)
		}
		if slots == nil {
			slots = make([][]byte, tot)
			total = tot
		} else if tot != total {
			return nil, fmt.Errorf("udp total mismatch: %w", ErrMalformedResponse)
		}
		if seq < 0 || seq >= total {
			return nil, fmt.Errorf("udp sequence out of range: %w", ErrMalformedResponse)
		}
		if slots[seq] != nil {
			continue
		}
		slots[seq] = dgram[udpHeaderSize:]
		got++
		if got == total {
			n := 0
			for _, p := range slots {
				n += len(p)
			}
			out := make([]byte, 0, n)
			for _, p := range slots {
				out = append(out, p...)
			}
			return out, nil
		}
	}
}

func protocolDataBlockSize(line []byte) (size uint64, has bool, err error) {
	trimmed := trimCRLF(line)
	switch {
	case bytes.HasPrefix(trimmed, []byte("VALUE ")):
		item := &Item{}
		size, err = parseValueLine(trimmed, item, true)
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
	if size > uint64(maxValueSize) {
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

// mapStorageReply maps a storage-class reply (set/add/replace/append/prepend/cas).
// STORED is success; NOT_STORED/EXISTS/NOT_FOUND stay protocol sentinels so the
// pooled conn is released. Anything else is malformed.
func mapStorageReply(resp *response) error {
	return mapStatusLine(resp, _StoredCRLFBytes)
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

func base64Encode(src []byte) []byte {
	dst := make([]byte, base64.StdEncoding.EncodedLen(len(src)))
	base64.StdEncoding.Encode(dst, src)
	return dst
}

func releaseReqAndResp(req *request, resp *response) {
	if req != nil {
		req.release()
	}

	if resp != nil {
		resp.release()
	}
}

func byteSliceToString(bs []byte) string {
	return string(bs)
}
