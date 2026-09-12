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
	extra     []string
}

func writeMetaDelete(b *protocolBuilder, key []byte, flags *metaDeleteFlags) {
	b.AddBytes(_MdBytes)
	b.addMetaKey(key, flags != nil && flags.b)
	if flags == nil {
		b.AddCRLF()
		return
	}
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
	b.appendExtraTokens(flags.extra)
	b.AddCRLF()
}

func encodeMetaDeleteOwned(key []byte, flags *metaDeleteFlags) (line []byte, box *[]byte) {
	b := newProtocolBuilder()
	defer b.release()
	writeMetaDelete(b, key, flags)
	return b.takeBytes()
}

// buildMetaDeleteCommand encodes md <key> <flags>*\r\n.
func buildMetaDeleteCommand(key []byte, flags *metaDeleteFlags) (*request, *response) {
	b := newProtocolBuilder()
	defer b.release()
	writeMetaDelete(b, key, flags)
	req := b.request(_MdBytes, key)

	resp := buildLimitedLineResponse(1)
	if flags != nil && flags.q {
		req.raw = appendMetaQuietBarrier(req.raw, MetaNoop{})
		resp = buildQuietBarrierResponse()
	}

	return req, resp
}
