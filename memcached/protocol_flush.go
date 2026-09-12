package memcached

// buildFlushAllCommand encodes flush_all [exptime] [extra…] [noreply]\r\n. ExptimeNever omits the token.
func buildFlushAllCommand(delay Exptime, extra []string, noReply bool) (*request, *response, error) {
	b := newProtocolBuilder().AddBytes(_FlushAllBytes)
	defer b.release()
	if delay != ExptimeNever {
		b.AddInt64(expiryToken(delay))
	}
	if err := b.appendTextTrail(extra, noReply); err != nil {
		return nil, nil, err
	}
	req := b.AddCRLF().request(_FlushAllBytes, nil)

	resp := buildLimitedLineResponse(1)
	if noReply {
		resp = buildNoReplyResponse()
	}

	return req, resp, nil
}
