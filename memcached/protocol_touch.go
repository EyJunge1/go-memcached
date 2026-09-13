package memcached

import "time"

// touch <key> <exptime> [noreply]\r\n
func buildTouchCommand(key string, expTime time.Duration, noReply bool) (*request, *response) {
	b := newProtocolBuilder().
		AddString("touch").
		AddString(key).
		AddInt64(expiryToken(expTime))
	defer b.release()

	if noReply {
		b.AddBytes(_NoReplyBytes)
	}

	req := buildRequest([]byte("touch"), []byte(key), b.AddCRLF().build())

	var resp *response
	if noReply {
		resp = buildNoReplyResponse()
	} else {
		resp = buildLimitedLineResponse(1)
	}

	return req, resp
}
