package memcached

import (
	"bytes"
	"fmt"
)

// metaDebugFlags is the encoded me flag set.
type metaDebugFlags struct {
	b bool // key is Base64
	P string
	L string
}

var (
	_expBytes   = []byte("exp")
	_laBytes    = []byte("la")
	_casBytes   = []byte("cas")
	_fetchBytes = []byte("fetch")
	_yesBytes   = []byte("yes")
	_clsBytes   = []byte("cls")
	_sizeBytes  = []byte("size")
)

// buildMetaDebugCommand encodes me <key> [b]\r\n.
func buildMetaDebugCommand(key []byte, flags *metaDebugFlags) (*request, *response) {
	if flags.b {
		key = base64Encode(key)
	}

	b := newProtocolBuilder().
		AddBytes(_MeBytes).
		AddBytes(key)
	defer b.release()

	b.AddFlagBool("b", flags.b)
	b.AddFlagString("P", flags.P)
	b.AddFlagString("L", flags.L)
	b.AddCRLF()

	req := b.request(_MeBytes, key)
	resp := buildLimitedLineResponse(1)

	return req, resp
}

// parseMetaItemDebug reads ME <key> key=value… or EN.
func parseMetaItemDebug(lines [][]byte, item *MetaItemDebug, binaryKey bool) error {
	if len(lines) != 1 {
		return fmt.Errorf("invalid response: %w", ErrMalformedResponse)
	}

	line := lines[0]
	if err := forecastCommonFaultLine(line); err != nil {
		return err
	}

	trimmed := trimCRLF(line)
	if len(trimmed) == 0 {
		return fmt.Errorf("invalid response: %w", ErrMalformedResponse)
	}
	cd := trimmed
	rest := []byte(nil)
	if i := bytes.IndexByte(trimmed, ' '); i >= 0 {
		cd = trimmed[:i]
		rest = trimmed[i+1:]
	}

	item.Status = metaStatusFromCD(cd)
	if err := item.Status.Err(); err != nil {
		return err
	}
	if !bytes.Equal(cd, _MEBytes) {
		return fmt.Errorf("unexpected cd<=%s>: %w", cd, ErrMalformedResponse)
	}

	if len(rest) == 0 {
		return nil
	}
	keyTok := rest
	fields := []byte(nil)
	if i := bytes.IndexByte(rest, ' '); i >= 0 {
		keyTok = rest[:i]
		fields = rest[i+1:]
	}
	item.WireKey = append(item.WireKey[:0], keyTok...)
	item.Key = item.WireKey
	item.KeyBase64 = binaryKey
	if binaryKey {
		dec, derr := base64Decode(item.WireKey)
		if derr != nil {
			return fmt.Errorf("invalid me b key: %w", ErrMalformedResponse)
		}
		item.Key = dec
	}
	if len(fields) == 0 {
		return nil
	}
	parseMetaDebugFields(fields, item)
	return nil
}

func parseMetaDebugFields(fields []byte, item *MetaItemDebug) {
	start := 0
	for i := 0; i <= len(fields); i++ {
		if i < len(fields) && fields[i] != ' ' {
			continue
		}
		if i == start {
			if i == len(fields) {
				return
			}
			start = i + 1
			continue
		}
		applyMetaDebugField(fields[start:i], item)
		start = i + 1
	}
}

func applyMetaDebugField(tok []byte, item *MetaItemDebug) {
	eq := bytes.IndexByte(tok, '=')
	if eq < 0 {
		return
	}
	key, val := tok[:eq], tok[eq+1:]
	switch {
	case bytes.Equal(key, _expBytes):
		item.TTL, _ = parseIntFromBytes(val)
	case bytes.Equal(key, _laBytes):
		item.LastAccess, _ = parseIntFromBytes(val)
	case bytes.Equal(key, _casBytes):
		item.CAS, _ = parseUintFromBytes(val)
	case bytes.Equal(key, _fetchBytes):
		item.HitBefore = bytes.Equal(val, _yesBytes)
	case bytes.Equal(key, _clsBytes):
		item.SlabClassID, _ = parseUintFromBytes(val)
	case bytes.Equal(key, _sizeBytes):
		item.Size, _ = parseUintFromBytes(val)
	default:
		if item.Fields == nil {
			item.Fields = make(map[string]string)
		}
		item.Fields[string(key)] = string(val)
	}
}
