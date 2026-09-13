package memcached

// metaDeleteFlags is the encoded md flag set.
type metaDeleteFlags struct {
	b      bool   // key is Base64
	C      uint64 // compare CAS
	E      uint64 // replacement CAS
	I      bool   // mark stale and bump CAS
	k      bool   // echo key
	O      uint64 // numeric opaque, echoed
	Otoken string
	q      bool  // quiet: skip HD; still return EX/NF
	T      int64 // TTL, only with I
	x      bool  // drop the value, keep the item

	Cset      bool
	Eset      bool
	Oset      bool
	OtokenSet bool
	Tset      bool
	P         string
	L         string
}

// buildMetaDeleteCommand encodes md <key> <flags>*\r\n.
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
		req.raw = appendMetaQuietBarrier(req.raw, MetaNoop{})
		resp = buildQuietBarrierResponse()
	} else {
		resp = buildLimitedLineResponse(1)
	}

	return req, resp
}
