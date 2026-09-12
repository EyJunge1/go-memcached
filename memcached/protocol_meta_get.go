package memcached

import (
	"bytes"
	"fmt"
)

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
	extra     []string
}

func writeMetaGet(b *protocolBuilder, key []byte, flags *metaGetFlags) {
	b.AddBytes(_MgBytes)
	b.addMetaKey(key, flags != nil && flags.b)
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
	b.appendExtraTokens(flags.extra)
	b.AddCRLF()
}

func writeMetaNoop(b *protocolBuilder, f MetaNoop) {
	b.AddBytes(_MnBytes)
	b.AddFlagString("P", f.ProxyHint)
	b.AddFlagString("L", f.ProxyPath)
	b.appendExtraTokens(f.Extra)
	b.AddCRLF()
}

func encodeMetaGetOwned(key []byte, flags *metaGetFlags) (wireKey, line []byte, box *[]byte) {
	b := newProtocolBuilder()
	defer b.release()
	writeMetaGet(b, key, flags)
	line, box = b.takeBytes()
	return key, line, box
}

// buildMetaGetCommand encodes mg <key> <flags>*\r\n. Quiet appends mn.
func buildMetaGetCommand(key []byte, flags *metaGetFlags) (*request, *response) {
	b := newProtocolBuilder()
	defer b.release()
	writeMetaGet(b, key, flags)
	if flags != nil && flags.q {
		writeMetaNoop(b, MetaNoop{})
	}
	req := b.request(_MgBytes, key)

	resp := buildLimitedLineResponse(1)
	if flags != nil && flags.q {
		resp = buildQuietBarrierResponse()
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
	body, err = dataBlockValue(body, size)
	if err != nil {
		return err
	}
	item.Value = ownValue(item.Value, body)
	return nil
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
