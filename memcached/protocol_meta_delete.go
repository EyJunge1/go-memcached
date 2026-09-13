package memcached

type metaDeleteFlags struct {
	b      bool   // b: interpret key as base64 encoded binary value
	C      uint64 // C(token): compare CAS value
	E      uint64 // E(token): use token as new CAS value
	I      bool   // I: invalidate. mark as stale, bumps CAS.
	k      bool   // k: return key
	O      uint64 // O(token): opaque to copy back.
	Otoken string
	q      bool  // q: noreply
	T      int64 // T(token): updates TTL, only when paired with the 'I' flag
	x      bool  // x: removes the item value, but leaves the item.

	Cset      bool
	Eset      bool
	Oset      bool
	OtokenSet bool
	Tset      bool
	P         string
	L         string
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
	b.AddFlagInt64Present("T", flags.T, flags.Tset)
	b.AddFlagBool("x", flags.x)
	b.AddFlagString("P", flags.P)
	b.AddFlagString("L", flags.L)

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
