package memcached

import (
	"testing"
)

func TestBuildMetaNoopCommand(t *testing.T) {
	req, resp := buildMetaNoopCommand(MetaNoop{})
	defer releaseReqAndResp(req, resp)
	equal(t, []byte("mn\r\n"), req.raw)
	equal(t, []byte("mn"), req.cmd)
	equal(t, _endIndicatorLimitedLines, resp.endIndicator)

	req, resp = buildMetaNoopCommand(MetaNoop{ProxyHint: "hint", ProxyPath: "/path"})
	defer releaseReqAndResp(req, resp)
	equal(t, []byte("mn Phint L/path\r\n"), req.raw)
}
