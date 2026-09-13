package memcached

import (
	"context"
	"sync"
	"time"
)

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
	req.cmd = nil
	req.key = nil
	req.raw = nil
	req.body = nil
	req.tail = nil
	req.udpEnabled = false
	req.udpReqID = 0

	requestPool.Put(req)
}

// send writes the request over TCP or UDP and flushes.
func (req *request) send(ctx context.Context, rr wireConn, writeTimeout time.Duration) (err error) {
	if has := selectProximateDeadline(ctx, rr, writeTimeout, clock, false); has {
		defer clearWriteDeadline(rr)
	}
	if ctx != nil && ctx.Done() != nil {
		stop := watchContextCancel(ctx, rr)
		defer stop()
	}

	if req.udpEnabled {
		return req.sendUDP(rr)
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

// watchContextCancel shortens the conn deadlines when ctx is canceled.
func watchContextCancel(ctx context.Context, rr wireConn) (stop func()) {
	if ctx == nil || ctx.Done() == nil {
		return nopStop
	}
	cancelWatch := context.AfterFunc(ctx, func() {
		now := clock()
		_ = rr.setReadDeadline(now)
		_ = rr.setWriteDeadline(now)
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
