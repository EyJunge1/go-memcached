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
// The zero value is MetaSetModeSet and omits M (server default is set).
type MetaSetMode uint8

const (
	// MetaSetModeSet overwrites regardless of prior existence. Zero Mode is set.
	MetaSetModeSet MetaSetMode = iota
	// MetaSetModeAdd stores only when the key is missing.
	MetaSetModeAdd
	// MetaSetModeReplace stores only when the key already exists.
	MetaSetModeReplace
	// MetaSetModeAppend appends to an existing value.
	MetaSetModeAppend
	// MetaSetModePrepend prepends to an existing value.
	MetaSetModePrepend
)

// metaSetModeToken maps Mode onto the wire M token. Set omits M.
func metaSetModeToken(m MetaSetMode) (string, bool) {
	switch m {
	case MetaSetModeSet:
		return "", false
	case MetaSetModeAdd:
		return "E", true
	case MetaSetModeReplace:
		return "R", true
	case MetaSetModeAppend:
		return "A", true
	case MetaSetModePrepend:
		return "P", true
	default:
		return "", false
	}
}

// validMetaSetMode reports whether m is a known ms mode.
func validMetaSetMode(m MetaSetMode) bool {
	switch m {
	case MetaSetModeSet, MetaSetModeAdd, MetaSetModeReplace, MetaSetModeAppend, MetaSetModePrepend:
		return true
	default:
		return false
	}
}

func writeMetaSet(b *protocolBuilder, key []byte, valueLen int, flags *metaSetFlags) {
	b.AddBytes(_MsBytes)
	if flags != nil && flags.b {
		b.AddBase64(key)
	} else {
		b.AddBytes(key)
	}
	b.AddInt(valueLen)
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
	if tok, ok := metaSetModeToken(flags.M); ok {
		b.AddFlagString("M", tok)
	}
	b.AddFlagInt64Present("N", flags.N, flags.Nset)
	b.AddFlagString("P", flags.P)
	b.AddFlagString("L", flags.L)
	b.AddCRLF()
}

func encodeMetaSetOwned(key, value []byte, flags *metaSetFlags) (header []byte, box *[]byte, body, tail []byte) {
	b := newProtocolBuilder()
	defer b.release()
	writeMetaSet(b, key, len(value), flags)
	header, box = b.takeBytes()
	return header, box, value, _CRLFBytes
}

// buildMetaSetCommand encodes ms <key> <datalen> <flags>*\r\n plus the body.
func buildMetaSetCommand(key, value []byte, flags *metaSetFlags) (*request, *response) {
	b := newProtocolBuilder()
	defer b.release()
	writeMetaSet(b, key, len(value), flags)
	req := b.request(_MsBytes, key)
	req.body = value

	var resp *response
	if flags.q {
		req.tail = _CRLFMnCRLFBytes
		resp = buildQuietBarrierResponse()
	} else {
		req.tail = _CRLFBytes
		resp = buildLimitedLineResponse(1)
	}

	return req, resp
}
