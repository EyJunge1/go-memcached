package memcached

// buildStorageCommand encodes <verb> <key> <flags> <exptime> <bytes> [noreply]\r\n plus the body.
func buildStorageCommand(
	command string,
	key []byte,
	value []byte,
	flags uint32,
	exptime Exptime,
	noReply bool,
) (*request, *response) {
	b := newProtocolBuilder().
		AddString(command).
		AddBytes(key).
		AddUint(uint64(flags)).
		AddInt64(expiryToken(exptime)).
		AddInt(len(value))
	defer b.release()

	if noReply {
		b.AddBytes(_NoReplyBytes)
	}

	b.AddCRLF()
	req := b.request(verbBytes(command), key)
	req.body = value
	req.tail = _CRLFBytes

	var resp *response
	if noReply {
		resp = buildNoReplyResponse()
	} else {
		resp = buildLimitedLineResponse(1)
	}

	return req, resp
}

// buildCasCommand encodes cas <key> <flags> <exptime> <bytes> <cas> [noreply]\r\n plus the body.
func buildCasCommand(
	key []byte, value []byte, flag uint32, expTime Exptime, casUnique uint64, noReply bool,
) (*request, *response) {
	b := newProtocolBuilder().
		AddString("cas").
		AddBytes(key).
		AddUint(uint64(flag)).
		AddInt64(expiryToken(expTime)).
		AddInt(len(value)).
		AddUint(casUnique)
	defer b.release()

	if noReply {
		b.AddBytes(_NoReplyBytes)
	}

	b.AddCRLF()
	req := b.request(_CasBytes, key)
	req.body = value
	req.tail = _CRLFBytes

	var resp *response
	if noReply {
		resp = buildNoReplyResponse()
	} else {
		resp = buildLimitedLineResponse(1)
	}

	return req, resp
}

// mapStorageReply maps a set/add/replace/append/prepend/cas reply.
func mapStorageReply(resp *response) error {
	return mapStatusLine(resp, _StoredCRLFBytes)
}
