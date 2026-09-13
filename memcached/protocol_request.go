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

type request struct {
	cmd []byte // command name
	key []byte // key is nil if the command DOES NOT need key
	raw []byte
	// body and tail are written after raw so storage values are not copied
	// into the builder buffer.
	body []byte
	tail []byte

	// this field is used to indicate whether the request is UDP enabled.
	// And it's set by the memcached client before sending the request.
	udpEnabled bool
	udpReqID   uint16
}

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
	req.body = nil
	req.tail = nil
	req.udpEnabled = false
	req.udpReqID = 0

	requestPool.Put(req)
}

func (req *request) send(ctx context.Context, rr wireConn, writeTimeout time.Duration) (err error) {
	if has := selectProximateDeadline(ctx, rr, writeTimeout, clock, false); has {
		defer func() { _ = rr.setWriteDeadline(zeroTime) }()
	}
	stop := watchContextCancel(ctx, rr)
	defer stop()

	if req.udpEnabled {
		return req.sendUDP(rr)
	}
	if err = req.write(rr); err != nil {
		return err
	}
	return rr.Flush()
}

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

func watchContextCancel(ctx context.Context, rr wireConn) (stop func()) {
	if ctx == nil {
		return func() {}
	}
	cancelWatch := context.AfterFunc(ctx, func() {
		now := clock()
		_ = rr.setReadDeadline(now)
		_ = rr.setWriteDeadline(now)
	})
	return func() { _ = cancelWatch() }
}

func releaseReqAndResp(req *request, resp *response) {
	if req != nil {
		req.release()
	}

	if resp != nil {
		resp.release()
	}
}
