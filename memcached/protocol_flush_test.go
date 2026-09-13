package memcached

import (
	"testing"
	"time"
)

func Test_buildFlushAllCommand_delay(t *testing.T) {
	req, resp := buildFlushAllCommand(0, false)
	defer releaseReqAndResp(req, resp)
	equal(t, []byte("flush_all\r\n"), req.raw)

	req, resp = buildFlushAllCommand(10, false)
	defer releaseReqAndResp(req, resp)
	equal(t, []byte("flush_all 10\r\n"), req.raw)

	req, resp = buildFlushAllCommand(Rel(10*time.Second), false)
	defer releaseReqAndResp(req, resp)
	equal(t, []byte("flush_all 10\r\n"), req.raw)

	req, resp = buildFlushAllCommand(ExptimeImmediate, false)
	defer releaseReqAndResp(req, resp)
	equal(t, []byte("flush_all -1\r\n"), req.raw)

	req, resp = buildFlushAllCommand(10, true)
	defer releaseReqAndResp(req, resp)
	equal(t, []byte("flush_all 10 noreply\r\n"), req.raw)
	equal(t, endIndicatorNoReply, resp.endIndicator)
}
