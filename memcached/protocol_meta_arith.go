package memcached

type metaArithmeticFlags struct {
	b      bool               // b: interpret key as base64 encoded binary value
	C      uint64             // C(token): compare CAS value
	E      uint64             // E(token): use token as new CAS value
	N      int64              // N(token): auto create item on miss with supplied TTL
	J      uint64             // J(token): initial value to use if auto created after miss (default 0)
	D      uint64             // D(token): delta to apply (decimal unsigned 64-bit number, default 1)
	T      int64              // T(token): update TTL on success
	M      metaArithmeticMode // M(token): mode switch to change between incr and decr modes.
	O      uint64             // O(token): opaque value, consumes a token and copies back with response
	Otoken string
	q      bool // q: use noreply semantics for return codes
	t      bool // t: return current TTL
	c      bool // c: return current CAS value if successful.
	v      bool // v: return new value
	k      bool // k: return key as a token

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

type metaArithmeticMode string

const (
	MetaArithmeticModeIncr metaArithmeticMode = "I"
	MetaArithmeticModeDecr metaArithmeticMode = "D"
)

func validMetaArithmeticMode(m metaArithmeticMode) bool {
	switch m {
	case "", MetaArithmeticModeIncr, MetaArithmeticModeDecr:
		return true
	default:
		return false
	}
}

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

	raw := b.AddCRLF().build()
	req := buildRequest([]byte("ma"), key, raw)

	var resp *response
	if flags.q {
		req.raw = appendMetaQuietBarrier(req.raw, MetaNoop{})
		resp = buildQuietBarrierResponse()
	} else {
		resp = buildLimitedLineResponse(1)
	}

	return req, resp
}
