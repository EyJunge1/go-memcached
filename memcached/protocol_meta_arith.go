package memcached

// metaArithmeticFlags is the encoded ma flag set.
type metaArithmeticFlags struct {
	b      bool               // key is Base64
	C      uint64             // compare CAS
	E      uint64             // replacement CAS
	N      int64              // create on miss (TTL)
	J      uint64             // initial value if created (default 0)
	D      uint64             // delta (default 1)
	T      int64              // TTL on success
	M      MetaArithmeticMode // incr or decr
	O      uint64             // numeric opaque, echoed
	Otoken string
	q      bool // quiet: skip HD/EN; still return EX/NF/NS
	t      bool // echo TTL
	c      bool // echo CAS
	v      bool // echo new value
	k      bool // echo key

	Cset      bool
	Eset      bool
	Nset      bool
	Jset      bool
	Tset      bool
	Oset      bool
	OtokenSet bool
	P         string
	L         string
}

// MetaArithmeticMode is the ma M token (incr or decr).
type MetaArithmeticMode string

const (
	MetaArithmeticModeIncr MetaArithmeticMode = "I"
	MetaArithmeticModeDecr MetaArithmeticMode = "D"
)

// validMetaArithmeticMode reports whether m is empty, incr, or decr.
func validMetaArithmeticMode(m MetaArithmeticMode) bool {
	switch m {
	case "", MetaArithmeticModeIncr, MetaArithmeticModeDecr:
		return true
	default:
		return false
	}
}

// buildMetaArithmeticCommand encodes ma <key> <flags>*\r\n. Quiet appends mn.
func buildMetaArithmeticCommand(key []byte, delta uint64, flags *metaArithmeticFlags) (*request, *response) {
	if flags.b {
		key = base64Encode(key)
	}
	flags.D = delta

	b := newProtocolBuilder().
		AddString("ma").
		AddBytes(key)
	defer b.release()

	b.AddFlagBool("b", flags.b)
	b.AddFlagUintPresent("C", flags.C, flags.Cset)
	b.AddFlagUintPresent("E", flags.E, flags.Eset)
	b.AddFlagInt64Present("N", flags.N, flags.Nset)
	b.AddFlagUintPresent("J", flags.J, flags.Jset)
	b.AddFlagUint("D", flags.D)
	b.AddFlagInt64Present("T", flags.T, flags.Tset)
	if validMetaArithmeticMode(flags.M) {
		b.AddFlagString("M", string(flags.M))
	}
	b.AddOpaque(flags.Otoken, flags.O, flags.Oset || flags.OtokenSet)
	b.AddFlagBool("q", flags.q)
	b.AddFlagBool("t", flags.t)
	b.AddFlagBool("c", flags.c)
	b.AddFlagBool("v", flags.v)
	b.AddFlagBool("k", flags.k)
	b.AddFlagString("P", flags.P)
	b.AddFlagString("L", flags.L)

	req := b.AddCRLF().request(_MaBytes, key)

	var resp *response
	if flags.q {
		req.raw = appendMetaQuietBarrier(req.raw, MetaNoop{})
		resp = buildQuietBarrierResponse()
	} else {
		resp = buildLimitedLineResponse(1)
	}

	return req, resp
}
