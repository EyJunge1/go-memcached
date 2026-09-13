package memcached

func buildFlushAllCommand(delay Exptime, noReply bool) (*request, *response) {
	b := newProtocolBuilder().AddString("flush_all")
	defer b.release()
	if delay != ExptimeNever {
		b.AddInt64(expiryToken(delay))
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
