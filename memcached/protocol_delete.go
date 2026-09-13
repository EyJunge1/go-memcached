package memcached

// buildDeleteCommand encodes delete <key> [noreply]\r\n.
func buildDeleteCommand(key []byte, noReply bool) (*request, *response) {
	b := newProtocolBuilder().
		AddString("delete").
		AddBytes(key)
	defer b.release()

	if noReply {
		b.AddBytes(_NoReplyBytes)
	}

	req := b.AddCRLF().request([]byte("delete"), key)

	var resp *response
	if noReply {
		resp = buildNoReplyResponse()
	} else {
		resp = buildLimitedLineResponse(1)
	}

	return req, resp
}
