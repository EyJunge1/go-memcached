package memcached

import (
	"testing"
)

func Test_buildMetaNoopCommand(t *testing.T) {
	req, resp := buildMetaNoopCommand()
	defer releaseReqAndResp(req, resp)
	equal(t, []byte("mn\r\n"), req.raw)
	equal(t, []byte("mn"), req.cmd)
	equal(t, endIndicatorLimitedLines, resp.endIndicator)
}
