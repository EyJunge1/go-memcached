package memcached

import (
	"context"
	"sync"
	"time"
)

const writevBodyThreshold = 2048

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

// request is one outbound frame.
type request struct {
	cmd []byte // verb, for UDP retry classification
	key []byte // routing key; nil when the command has none
	raw []byte
	// rawBox is the pool slot for raw when the builder detached it.
	rawBox *[]byte
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

// buildRequest returns a pooled request with cmd, key, and raw.
func buildRequest(cmd []byte, key []byte, raw []byte) *request {
	req := requestPool.Get().(*request)
	req.cmd = cmd
	req.key = key
	req.raw = raw
	return req
}

// release clears the request and returns it to the pool.
func (req *request) release() {
	if req.rawBox != nil {
		if cap(*req.rawBox) <= maxPooledBufferCap {
			*req.rawBox = (*req.rawBox)[:0]
			builderBufPool.Put(req.rawBox)
		}
		req.rawBox = nil
	} else {
		putBuilderBuf(req.raw)
	}
	putBuilderBox(req.keyBox)
	req.keyBox = nil
	req.cmd = nil
	req.key = nil
	req.raw = nil
	req.body = nil
	req.tail = nil
	req.udpEnabled = false
	req.udpReqID = 0

	requestPool.Put(req)
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
	return req.doSend(ctx, rr, writeTimeout, true)
}

// sendReady writes without arming or clearing deadlines. The caller already set them.
func (req *request) sendReady(ctx context.Context, rr wireConn) error {
	return req.doSend(ctx, rr, 0, false)
}

func (req *request) doSend(ctx context.Context, rr wireConn, writeTimeout time.Duration, armDeadline bool) (err error) {
	if armDeadline {
		if has := selectProximateDeadline(ctx, rr, writeTimeout, clock, false); has {
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
	if len(req.body) >= writevBodyThreshold {
		if c, ok := rr.(*conn); ok && c.wr != nil && c.wr.Buffered() == 0 {
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

func nopStop() {}

func clearWriteDeadline(rr wireConn) { _ = rr.setWriteDeadline(zeroTime) }

func clearReadDeadline(rr wireConn) { _ = rr.setReadDeadline(zeroTime) }

func clearDeadlines(rr wireConn) { _ = rr.setDeadline(zeroTime) }

// watchContextCancel shortens the conn deadlines when ctx is canceled.
func watchContextCancel(ctx context.Context, rr wireConn) (stop func()) {
	if ctx == nil || ctx.Done() == nil {
		return nopStop
	}
	cancelWatch := context.AfterFunc(ctx, func() {
		_ = rr.setDeadline(clock())
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
