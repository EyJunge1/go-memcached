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
// The zero value is MetaArithmeticModeIncr and omits M (server default is incr).
type MetaArithmeticMode uint8

const (
	// MetaArithmeticModeIncr adds the delta. Zero Mode is incr.
	MetaArithmeticModeIncr MetaArithmeticMode = iota
	// MetaArithmeticModeDecr subtracts the delta.
	MetaArithmeticModeDecr
)

// metaArithModeToken maps Mode onto the wire M token. Incr omits M.
func metaArithModeToken(m MetaArithmeticMode) (string, bool) {
	switch m {
	case MetaArithmeticModeIncr:
		return "", false
	case MetaArithmeticModeDecr:
		return "D", true
	default:
		return "", false
	}
}

// validMetaArithmeticMode reports whether m is incr or decr.
func validMetaArithmeticMode(m MetaArithmeticMode) bool {
	switch m {
	case MetaArithmeticModeIncr, MetaArithmeticModeDecr:
		return true
	default:
		return false
	}
}

func writeMetaArithmetic(b *protocolBuilder, key []byte, flags *metaArithmeticFlags) {
	b.AddBytes(_MaBytes)
	if flags != nil && flags.b {
		b.AddBase64(key)
	} else {
		b.AddBytes(key)
	}
	b.AddFlagBool("b", flags.b)
	b.AddFlagUintPresent("C", flags.C, flags.Cset)
	b.AddFlagUintPresent("E", flags.E, flags.Eset)
	b.AddFlagInt64Present("N", flags.N, flags.Nset)
	b.AddFlagUintPresent("J", flags.J, flags.Jset)
	b.AddFlagUint("D", flags.D)
	b.AddFlagInt64Present("T", flags.T, flags.Tset)
	if tok, ok := metaArithModeToken(flags.M); ok {
		b.AddFlagString("M", tok)
	}
	b.AddOpaque(flags.Otoken, flags.O, flags.Oset || flags.OtokenSet)
	b.AddFlagBool("q", flags.q)
	b.AddFlagBool("t", flags.t)
	b.AddFlagBool("c", flags.c)
	b.AddFlagBool("v", flags.v)
	b.AddFlagBool("k", flags.k)
	b.AddFlagString("P", flags.P)
	b.AddFlagString("L", flags.L)
	b.AddCRLF()
}

func encodeMetaArithmeticOwned(key []byte, delta uint64, flags *metaArithmeticFlags) (line []byte, box *[]byte) {
	flags.D = delta
	b := newProtocolBuilder()
	defer b.release()
	writeMetaArithmetic(b, key, flags)
	return b.takeBytes()
}

// buildMetaArithmeticCommand encodes ma <key> <flags>*\r\n. Quiet appends mn.
func buildMetaArithmeticCommand(key []byte, delta uint64, flags *metaArithmeticFlags) (*request, *response) {
	flags.D = delta

	b := newProtocolBuilder()
	defer b.release()
	writeMetaArithmetic(b, key, flags)
	req := b.request(_MaBytes, key)

	var resp *response
	if flags.q {
		req.raw = appendMetaQuietBarrier(req.raw, MetaNoop{})
		resp = buildQuietBarrierResponse()
	} else {
		resp = buildLimitedLineResponse(1)
	}

	return req, resp
}
