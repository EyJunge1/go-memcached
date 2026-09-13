package memcached

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
)

type metaGetFlags struct {
	b      bool   // b: interpret key as base64 encoded binary value
	c      bool   // c: return item cas token
	f      bool   // f: return client flags token
	h      bool   // h: return whether item has been hit before as a 0 or 1
	k      bool   // k: return key as a token
	l      bool   // l: return time since item was last accessed in seconds
	O      uint64 // O(token): opaque value, consumes a token and copies back with response
	Otoken string
	q      bool // q: use noreply semantics for return codes.
	s      bool // s: return item size token
	t      bool // t: return item TTL remaining in seconds (-1 for unlimited)
	u      bool // u: don't bump the item in the LRU
	v      bool // v: return item value in <data block>

	// These flags can modify the item:
	E uint64 // E(token): use token as new CAS value if item is modified
	N uint64 // N(token): vivify on miss, takes TTL as a argument
	R uint64 // R(token): if remaining TTL is less than token, win for recache
	T uint64 // T(token): update remaining TTL

	Oset      bool
	OtokenSet bool
	Eset      bool
	Nset      bool
	Rset      bool
	Tset      bool
	C         uint64
	Cset      bool

	// These extra flags can be added to the response:
	W bool // W: client has "won" the recache flag
	X bool // X: item is stale
	Z bool // Z: item has already sent a winning flag
}

// MetaGetOption is the option to set flags for meta get command.
type MetaGetOption func(*metaGetFlags)

// MetaGetFlagBinaryKey sets the flag to interpret key as base64 encoded binary value.
func MetaGetFlagBinaryKey() MetaGetOption {
	return func(flags *metaGetFlags) { flags.b = true }
}

// MetaGetFlagReturnCAS sets the flag to return item cas token.
func MetaGetFlagReturnCAS() MetaGetOption {
	return func(flags *metaGetFlags) { flags.c = true }
}

// MetaGetFlagReturnClientFlags sets the flag to return client flags token.
func MetaGetFlagReturnClientFlags() MetaGetOption {
	return func(flags *metaGetFlags) { flags.f = true }
}

// MetaGetFlagReturnHitBefore sets the flag to return whether item has been hit before as a 0 or 1.
func MetaGetFlagReturnHitBefore() MetaGetOption {
	return func(flags *metaGetFlags) { flags.h = true }
}

// MetaGetFlagReturnKey sets the flag to return key as a token.
func MetaGetFlagReturnKey() MetaGetOption {
	return func(flags *metaGetFlags) { flags.k = true }
}

// MetaGetFlagReturnLastAccessedTime sets the flag to return time since item was last accessed in seconds.
func MetaGetFlagReturnLastAccessedTime() MetaGetOption {
	return func(flags *metaGetFlags) { flags.l = true }
}

// MetaGetFlagOpaque sets the flag to opaque value, consumes a token and copies back with response.
func MetaGetFlagOpaque(token uint64) MetaGetOption {
	return func(flags *metaGetFlags) { flags.O = token; flags.Oset = true }
}

// MetaGetFlagOpaqueToken sets a string opaque token (1–32 bytes, no whitespace).
func MetaGetFlagOpaqueToken(token string) MetaGetOption {
	return func(flags *metaGetFlags) { flags.Otoken = token; flags.OtokenSet = true }
}

// MetaGetFlagCheckCAS requests a value only when the item CAS differs from cas.
// A match returns HD without a body (CASMatched on MetaItem).
func MetaGetFlagCheckCAS(cas uint64) MetaGetOption {
	return func(flags *metaGetFlags) { flags.C = cas; flags.Cset = true }
}

// MetaGetFlagNoReply sets the flag to use noreply semantics for return codes.
func MetaGetFlagNoReply() MetaGetOption {
	return func(flags *metaGetFlags) { flags.q = true }
}

// MetaGetFlagReturnSize sets the flag to return item size token.
func MetaGetFlagReturnSize() MetaGetOption {
	return func(flags *metaGetFlags) { flags.s = true }
}

// MetaGetFlagReturnTTL sets the flag to return item TTL remaining in seconds (-1 for unlimited).
func MetaGetFlagReturnTTL() MetaGetOption {
	return func(flags *metaGetFlags) { flags.t = true }
}

// MetaGetFlagDontBumpLRU sets the flag to don't bump the item in the LRU.
func MetaGetFlagDontBumpLRU() MetaGetOption {
	return func(flags *metaGetFlags) { flags.u = true }
}

// MetaGetFlagReturnValue sets the flag to return item value in <data block>.
func MetaGetFlagReturnValue() MetaGetOption {
	return func(flags *metaGetFlags) { flags.v = true }
}

// MetaGetFlagNewCAS sets the flag to use token as new CAS value if item is modified.
func MetaGetFlagNewCAS(casUnique uint64) MetaGetOption {
	return func(flags *metaGetFlags) { flags.E = casUnique; flags.Eset = true }
}

// MetaGetFlagVivifyOnMiss sets the flag to vivify on miss, takes TTL as an argument.
func MetaGetFlagVivifyOnMiss(ttl uint64) MetaGetOption {
	return func(flags *metaGetFlags) { flags.N = ttl; flags.Nset = true }
}

// MetaGetFlagWinForRecache sets the flag to if remaining TTL is less than token, win for recache.
func MetaGetFlagWinForRecache(ttl uint64) MetaGetOption {
	return func(flags *metaGetFlags) { flags.R = ttl; flags.Rset = true }
}

// MetaGetFlagUpdateRemainingTTL sets the flag to update remaining TTL.
func MetaGetFlagUpdateRemainingTTL(ttl uint64) MetaGetOption {
	return func(flags *metaGetFlags) { flags.T = ttl; flags.Tset = true }
}

// MetaGetFlagClientHasWonRecache is a no-op. W is a response-only flag.
//
// Deprecated: W is parsed onto MetaItem.Won; do not send it on requests.
func MetaGetFlagClientHasWonRecache() MetaGetOption {
	return func(*metaGetFlags) {}
}

// MetaGetFlagItemIsStale is a no-op. X is a response-only flag.
//
// Deprecated: X is parsed onto MetaItem.Stale; do not send it on requests.
func MetaGetFlagItemIsStale() MetaGetOption {
	return func(*metaGetFlags) {}
}

// MetaGetFlagItemHasSentWinningFlag is a no-op. Z is a response-only flag.
//
// Deprecated: Z is parsed onto MetaItem.RecacheBusy; do not send it on requests.
func MetaGetFlagItemHasSentWinningFlag() MetaGetOption {
	return func(*metaGetFlags) {}
}

func encodeMetaGetLine(key []byte, flags *metaGetFlags) ([]byte, []byte) {
	wireKey := key
	if flags.b {
		wireKey = base64Encode(key)
	}

	b := newProtocolBuilder().
		AddString("mg").
		AddBytes(wireKey)
	defer b.release()

	b.AddFlagBool("b", flags.b)
	b.AddFlagBool("c", flags.c)
	b.AddFlagBool("f", flags.f)
	b.AddFlagBool("h", flags.h)
	b.AddFlagBool("k", flags.k)
	b.AddFlagBool("l", flags.l)
	b.AddOpaque(flags.Otoken, flags.O, flags.Oset || flags.OtokenSet)
	b.AddFlagBool("q", flags.q)
	b.AddFlagBool("s", flags.s)
	b.AddFlagBool("t", flags.t)
	b.AddFlagBool("u", flags.u)
	b.AddFlagBool("v", flags.v)

	b.AddFlagUintPresent("C", flags.C, flags.Cset)
	b.AddFlagUintPresent("E", flags.E, flags.Eset)
	b.AddFlagUintPresent("N", flags.N, flags.Nset)
	b.AddFlagUintPresent("R", flags.R, flags.Rset)
	b.AddFlagUintPresent("T", flags.T, flags.Tset)

	return wireKey, b.AddCRLF().build()
}

// mg <key> <flags>*\r\n
func buildMetaGetCommand(key []byte, flags *metaGetFlags) (*request, *response) {
	wireKey, raw := encodeMetaGetLine(key, flags)
	req := buildRequest([]byte("mg"), wireKey, raw)

	var resp *response
	if flags.q {
		req.raw = appendMetaQuietBarrier(req.raw)
		resp = buildQuietBarrierResponse()
	} else {
		resp = buildLimitedLineResponse(1)
	}

	return req, resp
}

// parseMetaItem parses the response from memcached server. It coule be:
//
//	<CD> <flags>*\r\n
//
// .e.g:
//
//	HD c26 kZm9v b O456 s3\r\n
//
// CD is one of: HD(succeed), NF(not found), NS(not stored), EX(exists), EN(miss).
//
//	VA(value).
//
// VA is specified as: VA <size> <flags>*\r\n<data block>\r\n.
func parseMetaItem(lines [][]byte, item *MetaItem, noReply, wantValue bool) error {
	if err := forecastCollectedMeta(lines); err != nil {
		return err
	}

	proto := make([][]byte, 0, len(lines))
	for _, line := range lines {
		if bytes.Equal(line, _MetaMNCRLFBytes) {
			continue
		}
		proto = append(proto, line)
	}

	if noReply && len(proto) == 0 {
		if wantValue {
			return ErrNotFound
		}
		return nil
	}

	// figure out the <CD>
	if len(proto) == 0 {
		return fmt.Errorf("missing response: %w", ErrMalformedResponse)
	}
	lines = proto

	// Normal CD handling
	parts := bytes.Split(trimCRLF(lines[0]), _SpaceBytes)
	if len(parts) < 1 {
		return fmt.Errorf("invalid response: %w", ErrMalformedResponse)
	}
	const (
		CDIndex      = 0
		dataLenIndex = 1
	)
	cd := parts[CDIndex]

	switch string(cd) {
	case "NF":
		return ErrNotFound
	case "NS":
		return ErrNotStored
	case "EX":
		return ErrExists
	case "EN":
		return ErrNotFound
	}

	if !bytes.Equal(cd, []byte("VA")) {
		if bytes.Equal(cd, []byte("HD")) {
			item.gotHD = true
		}
		return parseFlags(parts, 1, item)
	}

	// VA handling
	if len(parts) <= dataLenIndex {
		return fmt.Errorf("missing value size: %w", ErrMalformedResponse)
	}
	size, err := strconv.ParseUint(string(parts[dataLenIndex]), 10, 64)
	if err != nil {
		return fmt.Errorf("invalid value size: %w", ErrMalformedResponse)
	}
	item.Size = size
	if err := parseFlags(parts, 2, item); err != nil {
		return err
	}

	if len(lines) < 2 {
		return fmt.Errorf("missing value: %w", ErrMalformedResponse)
	}

	item.Value, err = dataBlockValue(lines[1], size)
	if err != nil {
		return err
	}

	return nil
}

func forecastCollectedMeta(lines [][]byte) error {
	for _, line := range lines {
		if bytes.Equal(line, _MetaMNCRLFBytes) {
			continue
		}
		trimmed := trimCRLF(line)
		switch {
		case bytes.Equal(trimmed, []byte("ERROR")):
			return ErrNonexistentCommand
		case bytes.HasPrefix(trimmed, []byte("CLIENT_ERROR")):
			return forecastCommonFaultLine(line)
		case bytes.HasPrefix(trimmed, []byte("SERVER_ERROR")):
			return forecastCommonFaultLine(line)
		}
	}
	return nil
}

// CD <flags>*\r\n
// .e.g:
//
//	HD c26 kZm9v b O456 s3\r\n
func parseFlags(parts [][]byte, startPos int, item *MetaItem) error {
	parseUint := func(flag byte, b []byte) (uint64, error) {
		v, err := strconv.ParseUint(string(b), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid %c flag: %w", flag, ErrMalformedResponse)
		}
		return v, nil
	}

	parseInt := func(flag byte, b []byte) (int64, error) {
		v, err := strconv.ParseInt(string(b), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid %c flag: %w", flag, ErrMalformedResponse)
		}
		return v, nil
	}

	for i := startPos; i < len(parts); i++ {
		if len(parts[i]) == 0 {
			return fmt.Errorf("empty meta flag: %w", ErrMalformedResponse)
		}

		flag := parts[i][0]
		value := parts[i][1:]
		var err error
		switch flag {
		case 'c':
			item.CAS, err = parseUint(flag, value)
		case 'f':
			var flags uint64
			flags, err = parseUint(flag, value)
			if err == nil {
				if flags > math.MaxUint32 {
					return fmt.Errorf("f flag exceeds uint32: %w", ErrMalformedResponse)
				}
				item.Flags = uint32(flags)
			}
		case 't':
			item.TTL, err = parseInt(flag, value)
		case 'l':
			var lastAccessed uint64
			lastAccessed, err = parseUint(flag, value)
			if err == nil {
				if lastAccessed > math.MaxInt64 {
					return fmt.Errorf("l flag exceeds int64: %w", ErrMalformedResponse)
				}
				item.LastAccessedTime = int64(lastAccessed)
			}
		case 's':
			item.Size, err = parseUint(flag, value)
		case 'O':
			item.OpaqueToken = string(value)
			if v, perr := strconv.ParseUint(string(value), 10, 64); perr == nil {
				item.Opaque = v
			}
		case 'h':
			var hitBefore uint64
			hitBefore, err = parseUint(flag, value)
			item.HitBefore = hitBefore == 1
		case 'W':
			item.Won = true
		case 'X':
			item.Stale = true
		case 'Z':
			item.RecacheBusy = true
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func parseMetaPipeline(lines [][]byte) ([]*MetaItem, error) {
	if err := forecastCollectedMeta(lines); err != nil {
		return nil, err
	}
	var items []*MetaItem
	for i := 0; i < len(lines); {
		if bytes.Equal(lines[i], _MetaMNCRLFBytes) {
			i++
			continue
		}
		trimmed := trimCRLF(lines[i])
		parts := bytes.Split(trimmed, _SpaceBytes)
		if len(parts) == 0 {
			return nil, fmt.Errorf("empty meta line: %w", ErrMalformedResponse)
		}
		item := &MetaItem{}
		if bytes.Equal(parts[0], []byte("VA")) {
			if i+1 >= len(lines) {
				return nil, fmt.Errorf("missing value: %w", ErrMalformedResponse)
			}
			header, body := lines[i], lines[i+1] //nolint:gosec // G602: i+1 is bounds-checked above
			if err := parseMetaItem([][]byte{header, body}, item, false, false); err != nil {
				return nil, err
			}
			items = append(items, item)
			i += 2
			continue
		}
		flagStart := 1
		if err := parseFlags(parts, flagStart, item); err != nil {
			return nil, err
		}
		switch string(parts[0]) {
		case "HD":
			item.gotHD = true
			items = append(items, item)
			i++
		case "NF", "EN", "NS", "EX":
			items = append(items, item)
			i++
		default:
			if err := parseMetaItem([][]byte{lines[i]}, item, false, false); err != nil {
				return nil, err
			}
			items = append(items, item)
			i++
		}
	}
	return items, nil
}
