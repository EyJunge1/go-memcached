package memcached

import (
	"context"
	"sync"
	"time"
)

const _writevBodyThreshold = 2048

var _requestPool = sync.Pool{
	New: func() any {
		return &request{}
	},
}

// request is one outbound frame.
type request struct {
	cmd []byte // verb, for UDP retry classification
	key []byte // routing key; nil when the command has none
	raw []byte
	// rawBox is the pool slot for raw when the builder detached it.
	rawBox *[]byte
	// rawPut overrides the default small-builder pool for specialized batches.
	rawPut func(*[]byte)
	// keyBox is the pool slot for a namespaced routing key.
	keyBox *[]byte
	// body and tail follow raw so storage values skip the builder buffer.
	body []byte
	tail []byte

	// udpEnabled is set by the client before send when the node is UDP.
	udpEnabled bool
	udpReqID   uint16
}

// wire concatenates raw, body, and tail into one outbound frame.
func (req *request) wire() []byte {
	if len(req.body) == 0 && len(req.tail) == 0 {
		return req.raw
	}
	out := make([]byte, 0, len(req.raw)+len(req.body)+len(req.tail))
	out = append(out, req.raw...)
	out = append(out, req.body...)
	out = append(out, req.tail...)
	return out
}

// release clears the request and returns it to the pool.
func (req *request) release() {
	if req.rawBox != nil {
		if req.rawPut != nil {
			req.rawPut(req.rawBox)
		} else if cap(*req.rawBox) <= _maxPooledBufferCap {
			cleared := (*req.rawBox)[:0]
			if cleared == nil {
				cleared = _emptyBytes
			}
			*req.rawBox = cleared
			_builderBufPool.Put(req.rawBox)
		}
		req.rawBox = nil
	} else {
		putBuilderBuf(req.raw)
	}
	putBuilderBox(req.keyBox)
	req.keyBox = nil
	req.rawPut = nil
	req.cmd = nil
	req.key = nil
	req.raw = nil
	req.body = nil
	req.tail = nil
	req.udpEnabled = false
	req.udpReqID = 0

	_requestPool.Put(req)
}

// adoptKeyBox attaches a pooled namespaced key. A second box is returned to the pool.
func (req *request) adoptKeyBox(box *[]byte) {
	if box == nil {
		return
	}
	if req == nil || req.keyBox != nil {
		putBuilderBox(box)
		return
	}
	req.keyBox = box
}

// send writes the request over TCP or UDP and flushes.
func (req *request) send(ctx context.Context, rr wireConn, writeTimeout time.Duration) (err error) {
	const armDeadline = true
	return req.doSend(ctx, rr, writeTimeout, armDeadline)
}

// sendReady writes without arming or clearing deadlines. The caller already set them.
func (req *request) sendReady(ctx context.Context, rr wireConn) error {
	const armDeadline = false
	return req.doSend(ctx, rr, 0, armDeadline)
}

func (req *request) doSend(ctx context.Context, rr wireConn, writeTimeout time.Duration, armDeadline bool) (err error) {
	if armDeadline {
		const isRead = false
		if has := selectProximateDeadline(ctx, rr, writeTimeout, time.Now, isRead); has {
			defer clearWriteDeadline(rr)
		}
	}
	if armDeadline && ctx != nil && ctx.Done() != nil {
		stop := watchContextCancel(ctx, rr)
		defer stop()
	}

	if req.udpEnabled {
		return req.sendUDP(rr)
	}
	if c, ok := rr.(*conn); ok && c.wr != nil && c.wr.Buffered() == 0 && c.isTLS() {
		total := requestWireSize(req)
		if total >= _minTLSCoalesceBytes && total <= _maxTLSCoalesceBytes {
			reqs := [1]*request{req}
			_, err = c.writeTLSBatch(reqs[:], total)
			return err
		}
	}
	if len(req.body) >= _writevBodyThreshold {
		if c, ok := rr.(*conn); ok && c.wr != nil && c.wr.Buffered() == 0 &&
			c.supportsPipelineWritev() {
			return c.writeLargeBody(req.raw, req.body, req.tail)
		}
	}
	if err = req.write(rr); err != nil {
		return err
	}
	return rr.Flush()
}

// write writes raw, body, and tail without flushing.
func (req *request) write(rr wireConn) (err error) {
	if _, err = rr.Write(req.raw); err != nil {
		return err
	}
	if len(req.body) > 0 {
		if _, err = rr.Write(req.body); err != nil {
			return err
		}
	}
	if len(req.tail) > 0 {
		_, err = rr.Write(req.tail)
	}
	return err
}

// writePipeline writes one queued frame without the final batch flush.
// Bodies at least as large as the connection buffer bypass bufio after
// flushing earlier frames, avoiding a large copy and split header/body writes.
func (req *request) writePipeline(rr wireConn) error {
	if len(req.body) < _connWriteBufferSize {
		return req.write(rr)
	}
	c, ok := rr.(*conn)
	if !ok || c.wr == nil || !c.supportsPipelineWritev() {
		return req.write(rr)
	}
	if c.wr.Buffered() > 0 {
		if err := c.wr.Flush(); err != nil {
			return err
		}
	}
	return c.writeLargeBody(req.raw, req.body, req.tail)
}

// buildRequest returns a pooled request with cmd, key, and raw.
func buildRequest(cmd []byte, key []byte, raw []byte) *request {
	req, ok := _requestPool.Get().(*request)
	if !ok || req == nil {
		req = &request{}
	}
	req.cmd = cmd
	req.key = key
	req.raw = raw
	return req
}

func nopStop() {}

func clearWriteDeadline(rr wireConn) { _ = rr.setWriteDeadline(_zeroTime) }

func clearReadDeadline(rr wireConn) { _ = rr.setReadDeadline(_zeroTime) }

// watchContextCancel shortens the conn deadlines when ctx is canceled.
func watchContextCancel(ctx context.Context, rr wireConn) (stop func()) {
	if ctx == nil || ctx.Done() == nil {
		return nopStop
	}
	cancelWatch := context.AfterFunc(ctx, func() {
		_ = rr.setDeadline(time.Now())
	})
	return func() { _ = cancelWatch() }
}

// releaseReqAndResp releases both the request and the response.
func releaseReqAndResp(req *request, resp *response) {
	if req != nil {
		req.release()
	}

	if resp != nil {
		resp.release()
	}
}
