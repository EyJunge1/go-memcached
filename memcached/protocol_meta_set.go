package memcached

type metaSetFlags struct {
	b      bool        // b: interpret key as base64 encoded binary value (see metaget)
	c      bool        // c: return CAS value if successfully stored.
	C      uint64      // C(token): compare CAS value when storing item
	E      uint64      // E(token): use token as new CAS value (see meta get for detail)
	F      uint32      // F(token): set client flags to token (32 bit unsigned numeric)
	I      bool        // I: invalidate. set-to-invalid if supplied CAS is older than item's CAS
	k      bool        // k: return key as a token
	O      uint64      // O(token): opaque value, consumes a token and copies back with response
	Otoken string      // string opaque (1–32 bytes), takes precedence over O
	q      bool        // q: use noreply semantics for return codes
	s      bool        // s: return the size of the stored item on success (ie; new size on append)
	T      int64       // T(token): Time-To-Live for item, see "Expiration" above.
	M      metaSetMode // M(token): mode switch to change behavior to: add, replace, append, prepend, set(default)
	N      int64       // N(token): if in append mode, auto vivify on miss with supplied TTL

	Cset      bool
	Eset      bool
	Oset      bool
	OtokenSet bool
	Tset      bool
	Nset      bool
	P         string
	L         string
}

type metaSetMode string

const (
	// MetaSetModeAdd means add the item only if it does not already exist.
	MetaSetModeAdd metaSetMode = "E"
	// MetaSetModeReplace means replace the item only if it already exists.
	MetaSetModeReplace metaSetMode = "R"
	// MetaSetModeAppend means append the data to the existing item.
	MetaSetModeAppend metaSetMode = "A"
	// MetaSetModePrepend means prepend the data to the existing item.
	MetaSetModePrepend metaSetMode = "P"
	// MetaSetModeSet means set the item regardless of whether it already exists.
	MetaSetModeSet metaSetMode = "S"
)

func validMetaSetMode(m metaSetMode) bool {
	switch m {
	case "", MetaSetModeAdd, MetaSetModeReplace, MetaSetModeAppend, MetaSetModePrepend, MetaSetModeSet:
		return true
	default:
		return false
	}
}

// ms <key> <datalen> <flags>*\r\n
// <data block>\r\n
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
