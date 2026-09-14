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

func writeMetaDelete(b *protocolBuilder, key []byte, flags *metaDeleteFlags) {
	b.AddString("md").AddBytes(key)
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
	b.AddCRLF()
}

func encodeMetaDeleteOwned(key []byte, flags *metaDeleteFlags) (line []byte, box *[]byte) {
	if flags.b {
		key = base64Encode(key)
	}
	b := newProtocolBuilder()
	defer b.release()
	writeMetaDelete(b, key, flags)
	return b.takeBytes()
}

// buildMetaDeleteCommand encodes md <key> <flags>*\r\n.
func buildMetaDeleteCommand(key []byte, flags *metaDeleteFlags) (*request, *response) {
	if flags.b {
		key = base64Encode(key)
	}

	b := newProtocolBuilder()
	defer b.release()
	writeMetaDelete(b, key, flags)
	req := b.request(_MdBytes, key)

	var resp *response
	if flags.q {
		req.raw = appendMetaQuietBarrier(req.raw, MetaNoop{})
		resp = buildQuietBarrierResponse()
	} else {
		resp = buildLimitedLineResponse(1)
	}

	return req, resp
}
