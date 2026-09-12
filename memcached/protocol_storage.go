package memcached

// buildStorageCommand encodes <verb> <key> <flags> <exptime> <bytes> [extra…] [noreply]\r\n plus the body.
func buildStorageCommand(
	command []byte,
	key []byte,
	value []byte,
	flags uint32,
	exptime Exptime,
	extra []string,
	noReply bool,
) (*request, *response, error) {
	b := newProtocolBuilder().
		AddBytes(command).
		AddBytes(key).
		AddUint(uint64(flags)).
		AddInt64(expiryToken(exptime)).
		AddInt(len(value))
	defer b.release()

	if err := b.appendTextTrail(extra, noReply); err != nil {
		return nil, nil, err
	}
	b.AddCRLF()
	req := b.request(command, key)
	req.body = value
	req.tail = _CRLFBytes

	resp := buildLimitedLineResponse(1)
	if noReply {
		resp = buildNoReplyResponse()
	}

	return req, resp, nil
}

// buildCASCommand encodes cas <key> <flags> <exptime> <bytes> <cas> [extra…] [noreply]\r\n plus the body.
func buildCASCommand(
	key []byte, value []byte, flag uint32, expTime Exptime, casUnique uint64, extra []string, noReply bool,
) (*request, *response, error) {
	b := newProtocolBuilder().
		AddBytes(_CasBytes).
		AddBytes(key).
		AddUint(uint64(flag)).
		AddInt64(expiryToken(expTime)).
		AddInt(len(value)).
		AddUint(casUnique)
	defer b.release()

	if err := b.appendTextTrail(extra, noReply); err != nil {
		return nil, nil, err
	}
	b.AddCRLF()
	req := b.request(_CasBytes, key)
	req.body = value
	req.tail = _CRLFBytes

	resp := buildLimitedLineResponse(1)
	if noReply {
		resp = buildNoReplyResponse()
	}

	return req, resp, nil
}

// mapStorageReply maps a set/add/replace/append/prepend/cas reply.
func mapStorageReply(resp *response) error {
	return mapStatusLine(resp, _StoredCRLFBytes)
}
