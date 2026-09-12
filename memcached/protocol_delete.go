package memcached

// buildDeleteCommand encodes delete <key> [extra…] [noreply]\r\n.
func buildDeleteCommand(key []byte, extra []string, noReply bool) (*request, *response, error) {
	b := newProtocolBuilder().
		AddBytes(_DeleteBytes).
		AddBytes(key)
	defer b.release()

	if err := b.appendTextTrail(extra, noReply); err != nil {
		return nil, nil, err
	}
	req := b.AddCRLF().request(_DeleteBytes, key)

	resp := buildLimitedLineResponse(1)
	if noReply {
		resp = buildNoReplyResponse()
	}

	return req, resp, nil
}
