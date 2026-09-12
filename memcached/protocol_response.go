package memcached

import (
	"context"
	"sync"
	"time"
)

var _responsePool = sync.Pool{
	New: func() any {
		return &response{endIndicator: _endIndicatorNoReply}
	},
}

// responseEndIndicator chooses how recv drains a reply.
type responseEndIndicator uint8

const (
	// Zero responseEndIndicator is unset. Named values start at one.
	_endIndicatorUnknown responseEndIndicator = iota + 1
	// _endIndicatorNoReply means the client must not wait for a reply.
	_endIndicatorNoReply
	// _endIndicatorLimitedLines reads a fixed number of '\n'-terminated records.
	_endIndicatorLimitedLines
	// _endIndicatorSpecificEndLine reads '\n' records until specEndLine.
	_endIndicatorSpecificEndLine
	// _endIndicatorQuietBarrier reads until MN and keeps quiet error replies.
	_endIndicatorQuietBarrier
	// _endIndicatorRetrieval reads VALUE/data pairs until END into getDst.
	_endIndicatorRetrieval
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
	// udpBox holds the assembled datagram until release; udpFrame reads it.
	udpBox   *[]byte
	udpFrame byteFrame

	// afterRecv parses the frame before the socket returns to the pool.
	afterRecv func(*response) error

	// getDst is filled by _endIndicatorRetrieval. getKeys are the wired keys
	// for this node; getIdx maps local key i onto getDst (nil = identity).
	getDst    []GetResult
	getKeys   [][]byte
	getIdx    []int
	getOneKey []byte
	getOneDst *GetResult
	withCAS   bool

	// metaSlot is filled in-place for a single Meta Get/Set. metaItem points
	// at it (or a caller dest) so the item does not escape to a new heap alloc.
	metaSlot   MetaItem
	metaItem   *MetaItem
	metaFilled bool

	// metaPipe* stream Meta().Pipeline replies into dest by opaque (no VA clone).
	metaPipeC     *Client
	metaPipeDst   []MetaResult
	metaPipeGroup []preparedMetaOp
	metaPipeIndex *metaPipelineIndex
	metaPipeSeen  []byte
	metaSawReply  bool

	arithValue    uint64
	versionDst    *[]byte
	statsDst      *[]Stat
	statsFilled   bool
	statsDetail   bool
	statsBytes    int
	debugDst      *MetaItemDebug
	debugBinary   bool
	maxValueBytes int
	readTimeout   time.Duration
	readCtx       context.Context
	readConn      wireConn
}

func takeResponse() *response {
	resp, ok := _responsePool.Get().(*response)
	if !ok || resp == nil {
		return &response{endIndicator: _endIndicatorNoReply}
	}
	return resp
}

// buildNoReplyResponse returns a pooled response that waits for no reply.
func buildNoReplyResponse() *response {
	resp := takeResponse()
	resp.endIndicator = _endIndicatorNoReply
	return resp
}

// buildLimitedLineResponse returns a pooled response that reads a fixed number of lines.
func buildLimitedLineResponse(lines uint8) *response {
	resp := takeResponse()
	resp.endIndicator = _endIndicatorLimitedLines
	resp.limitedLines = lines
	resetRawLines(resp, int(lines))
	return resp
}

// buildSpecEndLineResponse returns a pooled response that reads until endLine.
func buildSpecEndLineResponse(endLine []byte, predictLines int) *response {
	if predictLines <= 0 {
		predictLines = 8
	}

	resp := takeResponse()
	resp.endIndicator = _endIndicatorSpecificEndLine
	resp.specEndLine = endLine
	resetRawLines(resp, predictLines)
	return resp
}

const _rawLinesKeepCap = 64

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
	resp.endIndicator = _endIndicatorUnknown
	resp.limitedLines = 0
	resp.specEndLine = nil
	for i := range resp.rawLines {
		resp.rawLines[i] = nil
	}
	resp.rawLines = resp.rawLines[:0]
	if cap(resp.rawLines) > _rawLinesKeepCap {
		resp.rawLines = make([][]byte, 0, _rawLinesKeepCap)
	}
	resp.udpEnabled = false
	resp.udpReqID = 0
	resp.udpFrame = byteFrame{}
	if resp.udpBox != nil {
		putUDPBuf(resp.udpBox)
		resp.udpBox = nil
	}
	resp.afterRecv = nil
	resp.getDst = nil
	resp.getKeys = nil
	resp.getIdx = nil
	resp.getOneKey = nil
	resp.getOneDst = nil
	resp.withCAS = false
	resp.metaSlot = MetaItem{}
	resp.metaItem = nil
	resp.metaFilled = false
	resp.metaPipeC = nil
	resp.metaPipeDst = nil
	resp.metaPipeGroup = nil
	putMetaPipelineIndex(resp.metaPipeIndex)
	resp.metaPipeIndex = nil
	resp.metaPipeSeen = resp.metaPipeSeen[:0]
	resp.metaSawReply = false
	resp.arithValue = 0
	resp.versionDst = nil
	resp.statsDst = nil
	resp.statsFilled = false
	resp.statsDetail = false
	resp.statsBytes = 0
	resp.debugDst = nil
	resp.debugBinary = false
	resp.maxValueBytes = 0
	resp.readTimeout = 0
	resp.readCtx = nil
	resp.readConn = nil
	_responsePool.Put(resp)
}

// buildRetrievalResponse returns a pooled response that parses VALUE pairs into dst.
func buildRetrievalResponse(keys [][]byte, origIdx []int, dst []GetResult, withCAS bool) *response {
	resp := takeResponse()
	resp.endIndicator = _endIndicatorRetrieval
	resp.withCAS = withCAS
	resp.getKeys = keys
	resp.getIdx = origIdx
	resp.getDst = dst
	return resp
}

// buildRetrievalResponseOne returns a pooled retrieval response that writes one slot.
func buildRetrievalResponseOne(key []byte, dst *GetResult, withCAS bool) *response {
	resp := takeResponse()
	resp.endIndicator = _endIndicatorRetrieval
	resp.withCAS = withCAS
	resp.getOneKey = key
	resp.getOneDst = dst
	return resp
}

func takeValueBuffer(dst []byte, n int) []byte {
	if n <= 0 {
		if dst == nil {
			return []byte{}
		}
		return dst[:0]
	}
	if cap(dst) >= n {
		return dst[:n]
	}
	return getSizedBytes(n)[:n]
}

func sameByteArray(a, b []byte) bool {
	if a == nil || b == nil || cap(a) == 0 || cap(b) == 0 {
		return false
	}
	return &a[:cap(a)][0] == &b[:cap(b)][0]
}

func ownValue(dst, src []byte) []byte {
	if len(src) == 0 {
		if dst != nil {
			return dst[:0]
		}
		return []byte{}
	}
	if dst != nil && cap(dst) >= len(src) {
		if sameByteArray(dst, src) {
			return src
		}
		dst = dst[:len(src)]
		copy(dst, src)
		return dst
	}
	if src == nil {
		return []byte{}
	}
	return src
}

// buildQuietBarrierResponse returns a response that reads until MN.
func buildQuietBarrierResponse() *response {
	resp := buildSpecEndLineResponse(_MetaMNCRLFBytes, 4)
	resp.endIndicator = _endIndicatorQuietBarrier
	return resp
}

// appendMetaQuietBarrier appends mn [P] [L] [Extra…] after raw.
func appendMetaQuietBarrier(raw []byte, f MetaNoop) []byte {
	if f.ProxyHint == "" && f.ProxyPath == "" && len(f.Extra) == 0 {
		return append(raw, _MnCRLFBytes...)
	}
	b := newProtocolBuilder().AddBytes(_MnBytes)
	defer b.release()
	b.AddFlagString("P", f.ProxyHint)
	b.AddFlagString("L", f.ProxyPath)
	b.appendExtraTokens(f.Extra)
	line, box := b.AddCRLF().takeBytes()
	raw = append(raw, line...)
	putBuilderBox(box)
	return raw
}
