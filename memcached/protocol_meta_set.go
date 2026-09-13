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
	T      uint64      // T(token): Time-To-Live for item, see "Expiration" above.
	M      metaSetMode // M(token): mode switch to change behavior to: add, replace, append, prepend, set(default)
	N      uint64      // N(token): if in append mode, auto vivify on miss with supplied TTL

	Cset      bool
	Eset      bool
	Oset      bool
	OtokenSet bool
	Tset      bool
	Nset      bool
}

// MetaSetOption is the option to set flags for meta set command.
type MetaSetOption func(*metaSetFlags)

// MetaSetFlagBinaryKey sets the flag to interpret key as binary value.
func MetaSetFlagBinaryKey() MetaSetOption {
	return func(flags *metaSetFlags) { flags.b = true }
}

// MetaSetFlagReturnCAS sets the flag to return CAS value if successfully stored.
func MetaSetFlagReturnCAS() MetaSetOption {
	return func(flags *metaSetFlags) { flags.c = true }
}

// MetaSetFlagCompareCAS sets the flag to compare CAS value when storing item.
func MetaSetFlagCompareCAS(casUnique uint64) MetaSetOption {
	return func(flags *metaSetFlags) { flags.C = casUnique; flags.Cset = true }
}

// MetaSetFlagNewCAS sets the flag to use token as new CAS value.
func MetaSetFlagNewCAS(casUnique uint64) MetaSetOption {
	return func(flags *metaSetFlags) { flags.E = casUnique; flags.Eset = true }
}

// MetaSetFlagClientFlags sets the flag to set client flags to token.
func MetaSetFlagClientFlags(flag uint32) MetaSetOption {
	return func(flags *metaSetFlags) {
		flags.F = flag
	}
}

// MetaSetFlagInvalidate sets the flag to set-to-invalid if supplied CAS is older than item's CAS.
func MetaSetFlagInvalidate() MetaSetOption {
	return func(flags *metaSetFlags) { flags.I = true }
}

// MetaSetFlagReturnKey sets the flag to return key as a token.
func MetaSetFlagReturnKey() MetaSetOption {
	return func(flags *metaSetFlags) { flags.k = true }
}

// MetaSetFlagOpaque sets the flag to opaque value, consumes a token and copies back with response.
func MetaSetFlagOpaque(token uint64) MetaSetOption {
	return func(flags *metaSetFlags) { flags.O = token; flags.Oset = true }
}

// MetaSetFlagOpaqueToken sets a string opaque token (1–32 bytes, no whitespace).
func MetaSetFlagOpaqueToken(token string) MetaSetOption {
	return func(flags *metaSetFlags) { flags.Otoken = token; flags.OtokenSet = true }
}

// MetaSetFlagNoReply sets the flag to use noreply semantics for return codes.
func MetaSetFlagNoReply() MetaSetOption {
	return func(flags *metaSetFlags) { flags.q = true }
}

// MetaSetFlagReturnSize sets the flag to return the size of the stored item on success.
func MetaSetFlagReturnSize() MetaSetOption {
	return func(flags *metaSetFlags) { flags.s = true }
}

// MetaSetFlagTTL sets the flag to Time-To-Live(seconds) for item.
// If the value is 0, the item will never expire.
func MetaSetFlagTTL(ttl uint64) MetaSetOption {
	return func(flags *metaSetFlags) { flags.T = ttl; flags.Tset = true }
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

// MetaSetFlagModeSwitch sets the flag to mode switch to change behavior to: add, replace, append, prepend, set(default).
func MetaSetFlagModeSwitch(mode metaSetMode) MetaSetOption {
	return func(flags *metaSetFlags) { flags.M = mode }
}

// MetaSetFlagAutoVivify sets the flag to auto vivify on miss with supplied TTL.
func MetaSetFlagAutoVivify(ttl uint64) MetaSetOption {
	return func(flags *metaSetFlags) { flags.N = ttl; flags.Nset = true }
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
	b.AddFlagUintPresent("T", flags.T, flags.Tset)
	if validMetaSetMode(flags.M) {
		b.AddFlagString("M", string(flags.M))
	}
	b.AddFlagUintPresent("N", flags.N, flags.Nset)

	raw := b.AddCRLF().
		AddBytes(value).
		AddCRLF().
		build()

	req := buildRequest([]byte("ms"), key, raw)

	var resp *response
	if flags.q {
		req.raw = appendMetaQuietBarrier(req.raw)
		resp = buildQuietBarrierResponse()
	} else {
		resp = buildLimitedLineResponse(1)
	}

	return req, resp
}
