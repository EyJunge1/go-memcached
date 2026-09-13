package memcached

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strconv"
)

var smallDecOpaque = [...]string{
	"0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
	"10", "11", "12", "13", "14", "15", "16", "17", "18", "19",
	"20", "21", "22", "23", "24", "25", "26", "27", "28", "29",
	"30", "31", "32", "33", "34", "35", "36", "37", "38", "39",
	"40", "41", "42", "43", "44", "45", "46", "47", "48", "49",
	"50", "51", "52", "53", "54", "55", "56", "57", "58", "59",
	"60", "61", "62", "63",
}

// metaGetFlags is the encoded mg flag set.
type metaGetFlags struct {
	b      bool   // key is Base64
	c      bool   // echo CAS
	f      bool   // echo client flags
	h      bool   // echo hit-before (0 or 1)
	k      bool   // echo key
	l      bool   // echo seconds since last access
	O      uint64 // numeric opaque, echoed
	Otoken string
	q      bool // quiet: skip HD/EN; still return EX/NF/NS
	s      bool // echo size
	t      bool // echo remaining TTL (-1 unlimited)
	u      bool // skip LRU bump
	v      bool // include value body

	E uint64 // new CAS if the item is written
	N int64  // create on miss (TTL)
	R int64  // recache win if remaining TTL is below token
	T int64  // set remaining TTL

	Oset      bool
	OtokenSet bool
	Eset      bool
	Nset      bool
	Rset      bool
	Tset      bool
	C         uint64
	Cset      bool
	P         string
	L         string
}

func metaGetWireKey(key []byte, flags *metaGetFlags) []byte {
	if flags != nil && flags.b {
		return base64Encode(key)
	}
	return key
}

func writeMetaGet(b *protocolBuilder, wireKey []byte, flags *metaGetFlags) {
	b.AddString("mg").AddBytes(wireKey)
	if flags == nil {
		b.AddCRLF()
		return
	}
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
	b.AddFlagInt64Present("N", flags.N, flags.Nset)
	b.AddFlagInt64Present("R", flags.R, flags.Rset)
	b.AddFlagInt64Present("T", flags.T, flags.Tset)
	b.AddFlagString("P", flags.P)
	b.AddFlagString("L", flags.L)
	b.AddCRLF()
}

func writeMetaNoop(b *protocolBuilder, f MetaNoop) {
	b.AddString("mn")
	b.AddFlagString("P", f.P)
	b.AddFlagString("L", f.L)
	b.AddCRLF()
}

func encodeMetaGetOwned(key []byte, flags *metaGetFlags) (wireKey, line []byte, box *[]byte) {
	wireKey = metaGetWireKey(key, flags)
	b := newProtocolBuilder()
	defer b.release()
	writeMetaGet(b, wireKey, flags)
	line, box = b.takeBytes()
	return wireKey, line, box
}

// buildMetaGetCommand encodes mg <key> <flags>*\r\n. Quiet appends mn.
func buildMetaGetCommand(key []byte, flags *metaGetFlags) (*request, *response) {
	wireKey := metaGetWireKey(key, flags)
	b := newProtocolBuilder()
	defer b.release()
	writeMetaGet(b, wireKey, flags)
	if flags != nil && flags.q {
		writeMetaNoop(b, MetaNoop{})
	}
	req := b.request(_MgBytes, wireKey)

	var resp *response
	if flags != nil && flags.q {
		resp = buildQuietBarrierResponse()
	} else {
		resp = buildLimitedLineResponse(1)
	}
	return req, resp
}

// parseMetaItem reads HD/VA/EN/NF/NS/EX plus optional flags and a VA body.
func parseMetaItem(lines [][]byte, item *MetaItem, noReply, quietMissAsNotFound bool) error {
	if err := forecastCollectedMeta(lines); err != nil {
		return err
	}

	firstIdx := -1
	n := 0
	for i, line := range lines {
		if isMetaNoopLine(line) {
			continue
		}
		if firstIdx < 0 {
			firstIdx = i
		}
		n++
	}

	if noReply && n == 0 {
		if quietMissAsNotFound {
			item.status = MetaStatusEN
			return ErrNotFound
		}
		item.status = MetaStatusHD
		item.gotHD = true
		return nil
	}
	if n == 0 {
		return fmt.Errorf("missing response: %w", ErrMalformedResponse)
	}

	cd, size, hasSize, err := parseMetaHeader(lines[firstIdx], item)
	if err != nil {
		return err
	}
	switch {
	case bytes.Equal(cd, _NFBytes), bytes.Equal(cd, _ENBytes):
		return ErrNotFound
	case bytes.Equal(cd, _NSBytes):
		return ErrNotStored
	case bytes.Equal(cd, _EXBytes):
		return ErrExists
	case bytes.Equal(cd, _HDBytes):
		item.gotHD = true
		return nil
	case bytes.Equal(cd, _MNBytes), bytes.Equal(cd, _MEBytes):
		return nil
	case !bytes.Equal(cd, _VABytes):
		return fmt.Errorf("unexpected meta cd %q: %w", cd, ErrMalformedResponse)
	}
	if !hasSize {
		return fmt.Errorf("missing value size: %w", ErrMalformedResponse)
	}
	var body []byte
	for i, line := range lines {
		if i == firstIdx || isMetaNoopLine(line) {
			continue
		}
		body = line
		break
	}
	if body == nil {
		return fmt.Errorf("missing value: %w", ErrMalformedResponse)
	}
	item.Value, err = dataBlockValue(body, size)
	return err
}

// forecastCollectedMeta returns the first meta fault that must abort parsing.
func forecastCollectedMeta(lines [][]byte) error {
	for i := 0; i < len(lines); i++ {
		if isMetaNoopLine(lines[i]) {
			continue
		}
		if err := forecastCommonFaultLine(lines[i]); err != nil && isChargeMetaFault(err) {
			return err
		}
		if bytes.HasPrefix(trimCRLF(lines[i]), _VASpaceBytes) {
			i++
		}
	}
	return nil
}

func parseMetaHeader(line []byte, item *MetaItem) (cd []byte, size uint64, hasSize bool, err error) {
	line = trimCRLF(line)
	if len(line) == 0 {
		return nil, 0, false, fmt.Errorf("invalid response: %w", ErrMalformedResponse)
	}
	end := bytes.IndexByte(line, ' ')
	if end < 0 {
		cd = line
	} else {
		cd = line[:end]
	}
	item.status = metaStatusFromCD(cd)
	if end < 0 {
		return cd, 0, false, nil
	}
	rest := line[end+1:]
	if bytes.Equal(cd, _VABytes) {
		sp := bytes.IndexByte(rest, ' ')
		var sizeTok []byte
		if sp < 0 {
			sizeTok = rest
			rest = nil
		} else {
			sizeTok = rest[:sp]
			rest = rest[sp+1:]
		}
		size, err = parseUintFromBytes(sizeTok)
		if err != nil {
			return cd, 0, false, fmt.Errorf("invalid value size: %w", ErrMalformedResponse)
		}
		item.Size = size
		hasSize = true
	}
	if len(rest) > 0 {
		if err = parseMetaFlagFields(rest, item); err != nil {
			return cd, size, hasSize, err
		}
	}
	return cd, size, hasSize, nil
}

func parseMetaFlagFields(fields []byte, item *MetaItem) error {
	start := 0
	for i := 0; i <= len(fields); i++ {
		if i < len(fields) && fields[i] != ' ' {
			continue
		}
		if i == start {
			if i == len(fields) {
				break
			}
			return fmt.Errorf("empty meta flag: %w", ErrMalformedResponse)
		}
		if err := applyMetaFlag(fields[start:i], item); err != nil {
			return err
		}
		start = i + 1
	}
	if item.KeyBase64 && len(item.WireKey) > 0 {
		dec, derr := base64Decode(item.WireKey)
		if derr != nil {
			return fmt.Errorf("invalid k/b key: %w", ErrMalformedResponse)
		}
		item.Key = dec
	}
	return nil
}

func applyMetaFlag(tok []byte, item *MetaItem) error {
	if len(tok) == 0 {
		return fmt.Errorf("empty meta flag: %w", ErrMalformedResponse)
	}
	flag := tok[0]
	value := tok[1:]
	switch flag {
	case 'c':
		n, err := parseUintFromBytes(value)
		if err != nil {
			return fmt.Errorf("invalid %c flag: %w", flag, ErrMalformedResponse)
		}
		item.CAS = n
	case 'f':
		n, err := parseUintFromBytes(value)
		if err != nil {
			return fmt.Errorf("invalid %c flag: %w", flag, ErrMalformedResponse)
		}
		if n > math.MaxUint32 {
			return fmt.Errorf("f flag exceeds uint32: %w", ErrMalformedResponse)
		}
		item.Flags = uint32(n)
	case 't':
		n, err := parseIntFromBytes(value)
		if err != nil {
			return fmt.Errorf("invalid %c flag: %w", flag, ErrMalformedResponse)
		}
		item.TTL = n
	case 'l':
		n, err := parseUintFromBytes(value)
		if err != nil {
			return fmt.Errorf("invalid %c flag: %w", flag, ErrMalformedResponse)
		}
		if n > math.MaxInt64 {
			return fmt.Errorf("l flag exceeds int64: %w", ErrMalformedResponse)
		}
		item.LastAccess = int64(n)
	case 's':
		n, err := parseUintFromBytes(value)
		if err != nil {
			return fmt.Errorf("invalid %c flag: %w", flag, ErrMalformedResponse)
		}
		item.Size = n
	case 'O':
		if n, err := parseUintFromBytes(value); err == nil && opaqueIsCanonical(value, n) {
			item.opaqueNum = n
			item.hasOpaqueNum = true
			item.Opaque = internDecOpaque(n)
		} else {
			item.Opaque = string(value)
		}
	case 'h':
		n, err := parseUintFromBytes(value)
		if err != nil {
			return fmt.Errorf("invalid %c flag: %w", flag, ErrMalformedResponse)
		}
		item.HitBefore = n == 1
	case 'k':
		item.WireKey = append([]byte(nil), value...)
		item.Key = item.WireKey
	case 'b':
		item.KeyBase64 = true
	case 'W':
		item.Won = true
	case 'X':
		item.Stale = true
	case 'Z':
		item.RecacheBusy = true
	}
	return nil
}

func internDecOpaque(n uint64) string {
	if n < uint64(len(smallDecOpaque)) {
		return smallDecOpaque[n]
	}
	return strconv.FormatUint(n, 10)
}

func opaqueIsCanonical(b []byte, n uint64) bool {
	if n == 0 {
		return len(b) == 1 && b[0] == '0'
	}
	var buf [20]byte
	return bytes.Equal(b, strconv.AppendUint(buf[:0], n, 10))
}

// parseFlags applies meta reply tokens (c, f, t, l, s, O, h, W, X, Z, k, b).
func parseFlags(parts [][]byte, startPos int, item *MetaItem) error {
	for i := startPos; i < len(parts); i++ {
		if len(parts[i]) == 0 {
			return fmt.Errorf("empty meta flag: %w", ErrMalformedResponse)
		}
		if err := applyMetaFlag(parts[i], item); err != nil {
			return err
		}
	}
	if item.KeyBase64 && len(item.WireKey) > 0 {
		dec, derr := base64Decode(item.WireKey)
		if derr != nil {
			return fmt.Errorf("invalid k/b key: %w", ErrMalformedResponse)
		}
		item.Key = dec
	}
	return nil
}

// parseMetaPipeline parses a pipelined meta reply into items.
func parseMetaPipeline(lines [][]byte) ([]MetaItem, error) {
	items := make([]MetaItem, 0, len(lines))
	var charge error
	for i := 0; i < len(lines); {
		if isMetaNoopLine(lines[i]) {
			i++
			continue
		}
		if err := forecastCommonFaultLine(lines[i]); err != nil && isChargeMetaFault(err) {
			if charge == nil {
				charge = err
			}
			i++
			continue
		}
		var item MetaItem
		cd := trimCRLF(lines[i])
		if sp := bytes.IndexByte(cd, ' '); sp >= 0 {
			cd = cd[:sp]
		}
		if bytes.Equal(cd, _VABytes) {
			if i+1 >= len(lines) {
				return nil, fmt.Errorf("missing value: %w", ErrMalformedResponse)
			}
			header, body := lines[i], lines[i+1] //nolint:gosec // G602: i+1 is bounds-checked above
			if err := parseMetaItem([][]byte{header, body}, &item, false, false); err != nil {
				return nil, err
			}
			items = append(items, item)
			i += 2
			continue
		}
		if err := parseMetaItem([][]byte{lines[i]}, &item, false, false); err != nil &&
			!errors.Is(err, ErrNotFound) && !errors.Is(err, ErrNotStored) && !errors.Is(err, ErrExists) {
			return nil, err
		}
		items = append(items, item)
		i++
	}
	return items, charge
}
