package memcached

// buildTouchCommand encodes touch <key> <exptime> [noreply]\r\n.
func buildTouchCommand(key []byte, expTime Exptime, noReply bool) (*request, *response) {
	b := newProtocolBuilder().
		AddString("touch").
		AddBytes(key).
		AddInt64(expiryToken(expTime))
	defer b.release()

	if noReply {
		b.AddBytes(_NoReplyBytes)
	}

	req := b.AddCRLF().request([]byte("touch"), key)

	var resp *response
	if noReply {
		resp = buildNoReplyResponse()
	} else {
		resp = buildLimitedLineResponse(1)
	}

	return req, resp
}
