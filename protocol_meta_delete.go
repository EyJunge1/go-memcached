package memcached

// MetaDeleteOption is used to set options for MetaDelete command.
type MetaDeleteOption func(*metaDeleteFlags)

type metaDeleteFlags struct {
	b      bool   // b: interpret key as base64 encoded binary value
	C      uint64 // C(token): compare CAS value
	E      uint64 // E(token): use token as new CAS value
	I      bool   // I: invalidate. mark as stale, bumps CAS.
	k      bool   // k: return key
	O      uint64 // O(token): opaque to copy back.
	Otoken string
	q      bool   // q: noreply
	T      uint64 // T(token): updates TTL, only when paired with the 'I' flag
	x      bool   // x: removes the item value, but leaves the item.

	Cset      bool
	Eset      bool
	Oset      bool
	OtokenSet bool
	Tset      bool
}

// MetaDeleteFlagBinaryKey sets the flag to interpret key as base64 encoded binary value.
func MetaDeleteFlagBinaryKey() MetaDeleteOption {
	return func(flags *metaDeleteFlags) { flags.b = true }
}

// MetaDeleteFlagCompareCAS sets the flag to compare CAS value.
func MetaDeleteFlagCompareCAS(casUnique uint64) MetaDeleteOption {
	return func(flags *metaDeleteFlags) { flags.C = casUnique; flags.Cset = true }
}

// MetaDeleteFlagNewCAS sets the flag to use token as new CAS value.
func MetaDeleteFlagNewCAS(casUnique uint64) MetaDeleteOption {
	return func(flags *metaDeleteFlags) { flags.E = casUnique; flags.Eset = true }
}

// MetaDeleteFlagInvalidate sets the flag to invalidate. mark as stale, bumps CAS.
func MetaDeleteFlagInvalidate() MetaDeleteOption {
	return func(flags *metaDeleteFlags) { flags.I = true }
}

// MetaDeleteFlagReturnKey sets the flag to return key.
func MetaDeleteFlagReturnKey() MetaDeleteOption {
	return func(flags *metaDeleteFlags) { flags.k = true }
}

// MetaDeleteFlagOpaque sets the flag to opaque to copy back.
func MetaDeleteFlagOpaque(token uint64) MetaDeleteOption {
	return func(flags *metaDeleteFlags) { flags.O = token; flags.Oset = true }
}

// MetaDeleteFlagOpaqueToken sets a string opaque token (1–32 bytes, no whitespace).
func MetaDeleteFlagOpaqueToken(token string) MetaDeleteOption {
	return func(flags *metaDeleteFlags) { flags.Otoken = token; flags.OtokenSet = true }
}

// MetaDeleteFlagNoReply sets the flag to noreply.
func MetaDeleteFlagNoReply() MetaDeleteOption {
	return func(flags *metaDeleteFlags) { flags.q = true }
}

// MetaDeleteFlagUpdateTTL sets the flag to updates TTL,
// only when paired with the 'I' flag (MetaDeleteFlagInvalidate).
func MetaDeleteFlagUpdateTTL(ttl uint64) MetaDeleteOption {
	return func(flags *metaDeleteFlags) { flags.T = ttl; flags.Tset = true }
}

// MetaDeleteFlagRemoveValueOnly sets the flag to
// removes the item value, but leaves the item.
func MetaDeleteFlagRemoveValueOnly() MetaDeleteOption {
	return func(flags *metaDeleteFlags) { flags.x = true }
}

// md <key> <flags>*\r\n
func buildMetaDeleteCommand(key []byte, flags *metaDeleteFlags) (*request, *response) {
	if flags.b {
		key = base64Encode(key)
	}

	b := newProtocolBuilder().
		AddString("md").
		AddBytes(key)
	defer b.release()

	b.AddFlagBool("b", flags.b)
	b.AddFlagUintPresent("C", flags.C, flags.Cset)
	b.AddFlagUintPresent("E", flags.E, flags.Eset)
	b.AddFlagBool("I", flags.I)
	b.AddFlagBool("k", flags.k)
	b.AddOpaque(flags.Otoken, flags.O, flags.Oset || flags.OtokenSet)
	b.AddFlagBool("q", flags.q)
	b.AddFlagUintPresent("T", flags.T, flags.Tset)
	b.AddFlagBool("x", flags.x)

	raw := b.AddCRLF().
		build()

	req := buildRequest([]byte("md"), key, raw)

	var resp *response
	if flags.q {
		req.raw = appendMetaQuietBarrier(req.raw)
		resp = buildQuietBarrierResponse()
	} else {
		resp = buildLimitedLineResponse(1)
	}

	return req, resp
}
