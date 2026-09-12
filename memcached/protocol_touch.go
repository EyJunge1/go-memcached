package memcached

// buildTouchCommand encodes touch <key> <exptime> [extra…] [noreply]\r\n.
func buildTouchCommand(key []byte, expTime Exptime, extra []string, noReply bool) (*request, *response, error) {
	b := newProtocolBuilder().
		AddBytes(_TouchBytes).
		AddBytes(key).
		AddInt64(expiryToken(expTime))
	defer b.release()

	if err := b.appendTextTrail(extra, noReply); err != nil {
		return nil, nil, err
	}
	req := b.AddCRLF().request(_TouchBytes, key)

	resp := buildLimitedLineResponse(1)
	if noReply {
		resp = buildNoReplyResponse()
	}

	return req, resp, nil
}
