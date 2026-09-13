package memcached

import "time"

func buildFlushAllCommand(delay time.Duration, noReply bool) (*request, *response) {
	b := newProtocolBuilder().AddString("flush_all")
	defer b.release()
	if delay > 0 {
		sec := delay / time.Second
		if sec == 0 {
			sec = 1
		}
		b.AddInt64(int64(sec))
	}
	if noReply {
		b.AddBytes(_NoReplyBytes)
	}
	req := buildRequest([]byte("flush_all"), nil, b.AddCRLF().build())

	var resp *response
	if noReply {
		resp = buildNoReplyResponse()
	} else {
		resp = buildLimitedLineResponse(1)
	}

	return req, resp
}
