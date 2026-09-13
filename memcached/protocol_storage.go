package memcached

// buildStorageCommand constructs storage class command, including:
//
//	set/add/replace/append/prepend, etc.
//
// <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
// <data block>\r\n
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

	raw := b.AddCRLF().build()
	req := buildRequest([]byte(command), key, raw)
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

// cas <key> <flag> <exptime> <bytes> <cas unique> [noreply]\r\n
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

	raw := b.AddCRLF().build()
	req := buildRequest([]byte("cas"), key, raw)
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

// mapStorageReply maps a storage-class reply (set/add/replace/append/prepend/cas).
// STORED is success; NOT_STORED/EXISTS/NOT_FOUND stay protocol sentinels so the
// pooled conn is released. Anything else is malformed.
func mapStorageReply(resp *response) error {
	return mapStatusLine(resp, _StoredCRLFBytes)
}
