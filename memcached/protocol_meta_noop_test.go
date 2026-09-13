package memcached

import (
	"testing"
)

func Test_buildMetaNoopCommand(t *testing.T) {
	req, resp := buildMetaNoopCommand(MetaNoop{})
	defer releaseReqAndResp(req, resp)
	equal(t, []byte("mn\r\n"), req.raw)
	equal(t, []byte("mn"), req.cmd)
	equal(t, endIndicatorLimitedLines, resp.endIndicator)

	req, resp = buildMetaNoopCommand(MetaNoop{P: "hint", L: "/path"})
	defer releaseReqAndResp(req, resp)
	equal(t, []byte("mn Phint L/path\r\n"), req.raw)
}
