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

func releaseReqAndResp(req *request, resp *response) {
	if req != nil {
		req.release()
	}

	if resp != nil {
		resp.release()
	}
}
