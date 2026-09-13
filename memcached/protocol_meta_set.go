package memcached

// metaSetFlags is the encoded ms flag set.
type metaSetFlags struct {
	b      bool        // key is Base64
	c      bool        // echo CAS on store
	C      uint64      // compare CAS
	E      uint64      // replacement CAS
	F      uint32      // client flags
	I      bool        // invalidate if supplied CAS is older
	k      bool        // echo key
	O      uint64      // numeric opaque, echoed
	Otoken string      // string opaque (1–32 bytes) wins over O
	q      bool        // quiet: skip HD; still return EX/NS
	s      bool        // echo stored size
	T      int64       // item TTL
	M      MetaSetMode // add/replace/append/prepend/set
	N      int64       // append/prepend create-on-miss TTL

	Cset      bool
	Eset      bool
	Oset      bool
	OtokenSet bool
	Tset      bool
	Nset      bool
	P         string
	L         string
}

// MetaSetMode is the ms M token (add, replace, append, prepend, set).
type MetaSetMode string

const (
	// MetaSetModeAdd stores only when the key is missing.
	MetaSetModeAdd MetaSetMode = "E"
	// MetaSetModeReplace stores only when the key already exists.
	MetaSetModeReplace MetaSetMode = "R"
	// MetaSetModeAppend appends to an existing value.
	MetaSetModeAppend MetaSetMode = "A"
	// MetaSetModePrepend prepends to an existing value.
	MetaSetModePrepend MetaSetMode = "P"
	// MetaSetModeSet overwrites regardless of prior existence.
	MetaSetModeSet MetaSetMode = "S"
)

// validMetaSetMode reports whether m is empty or a known ms mode.
func validMetaSetMode(m MetaSetMode) bool {
	switch m {
	case "", MetaSetModeAdd, MetaSetModeReplace, MetaSetModeAppend, MetaSetModePrepend, MetaSetModeSet:
		return true
	default:
		return false
	}
}

// buildMetaSetCommand encodes ms <key> <datalen> <flags>*\r\n plus the body.
func buildMetaSetCommand(key, value []byte, flags *metaSetFlags) (*request, *response) {
	if flags.b {
		key = base64Encode(key)
	}

	b := newProtocolBuilder().
		AddString("ms").
		AddBytes(key).
		AddInt(len(value))
	defer b.release()

	b.AddFlagBool("b", flags.b)
	b.AddFlagBool("c", flags.c)
	b.AddFlagUintPresent("C", flags.C, flags.Cset)
	b.AddFlagUintPresent("E", flags.E, flags.Eset)
	b.AddFlagUintNonZero("F", uint64(flags.F))
	b.AddFlagBool("I", flags.I)
	b.AddFlagBool("k", flags.k)
	b.AddOpaque(flags.Otoken, flags.O, flags.Oset || flags.OtokenSet)
	b.AddFlagBool("q", flags.q)
	b.AddFlagBool("s", flags.s)
	b.AddFlagInt64Present("T", flags.T, flags.Tset)
	if validMetaSetMode(flags.M) {
		b.AddFlagString("M", string(flags.M))
	}
	b.AddFlagInt64Present("N", flags.N, flags.Nset)
	b.AddFlagString("P", flags.P)
	b.AddFlagString("L", flags.L)

	raw := b.AddCRLF().build()
	req := buildRequest([]byte("ms"), key, raw)
	req.body = value

	var resp *response
	if flags.q {
		req.tail = []byte("\r\nmn\r\n")
		resp = buildQuietBarrierResponse()
	} else {
		req.tail = _CRLFBytes
		resp = buildLimitedLineResponse(1)
	}

	return req, resp
}
