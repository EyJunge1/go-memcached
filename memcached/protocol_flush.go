package memcached

// buildFlushAllCommand encodes flush_all [exptime] [noreply]\r\n. ExptimeNever omits the token.
func buildFlushAllCommand(delay Exptime, noReply bool) (*request, *response) {
	b := newProtocolBuilder().AddString("flush_all")
	defer b.release()
	if delay != ExptimeNever {
		b.AddInt64(expiryToken(delay))
	}
	if noReply {
		b.AddBytes(_NoReplyBytes)
	}
	req := b.AddCRLF().request([]byte("flush_all"), nil)

	var resp *response
	if noReply {
		resp = buildNoReplyResponse()
	} else {
		resp = buildLimitedLineResponse(1)
	}

	return req, resp
}
