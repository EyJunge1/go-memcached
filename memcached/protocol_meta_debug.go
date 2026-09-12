package memcached

import (
	"bytes"
	"fmt"
	"time"
)

// metaDebugFlags is the encoded me flag set.
type metaDebugFlags struct {
	b     bool // key is Base64
	P     string
	L     string
	extra []string
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
	b := newProtocolBuilder()
	defer b.release()
	b.AddBytes(_MeBytes)
	b.addMetaKey(key, flags != nil && flags.b)
	if flags != nil {
		b.AddFlagBool("b", flags.b)
		b.AddFlagString("P", flags.P)
		b.AddFlagString("L", flags.L)
		b.appendExtraTokens(flags.extra)
	}
	b.AddCRLF()

	req := b.request(_MeBytes, key)
	resp := buildLimitedLineResponse(1)

	return req, resp
}

// mapDebugReply parses me into debugDst while the connection is still held.
func mapDebugReply(resp *response) error {
	if resp.debugDst == nil {
		return fmt.Errorf("meta debug dst: %w", ErrInvalidArgument)
	}
	err := parseMetaItemDebug(resp.rawLines, resp.debugDst, resp.debugBinary)
	if err != nil && resp.debugDst.Status.Err() != nil {
		return nil
	}
	return err
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
	var rest []byte
	if before, after, ok := bytes.Cut(trimmed, []byte{' '}); ok {
		cd = before
		rest = after
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
	var fields []byte
	if i := bytes.IndexByte(rest, ' '); i >= 0 {
		keyTok = rest[:i]
		fields = rest[i+1:]
	}
	item.WireKey = append(item.WireKey[:0], keyTok...)
	item.KeyBase64 = binaryKey
	item.Key = item.WireKey
	if binaryKey {
		dst := item.Key
		if sameByteArray(dst, item.WireKey) {
			dst = nil
		}
		dec, derr := base64DecodeInto(dst, item.WireKey)
		if derr != nil {
			return fmt.Errorf("invalid me b key: %w", ErrMalformedResponse)
		}
		item.Key = dec
	}
	if len(fields) == 0 {
		return nil
	}
	return parseMetaDebugFields(fields, item)
}

func parseMetaDebugFields(fields []byte, item *MetaItemDebug) error {
	start := 0
	for i := 0; i <= len(fields); i++ {
		if i < len(fields) && fields[i] != ' ' {
			continue
		}
		if i == start {
			if i == len(fields) {
				return nil
			}
			start = i + 1
			continue
		}
		if err := applyMetaDebugField(fields[start:i], item); err != nil {
			return err
		}
		start = i + 1
	}
	return nil
}

func applyMetaDebugField(tok []byte, item *MetaItemDebug) error {
	before, after, ok := bytes.Cut(tok, []byte{'='})
	if !ok {
		return nil
	}
	key, val := before, after
	var err error
	switch {
	case bytes.Equal(key, _expBytes):
		var sec int64
		sec, err = parseIntFromBytes(val)
		if err == nil {
			item.TTL = time.Duration(sec) * time.Second
		}
	case bytes.Equal(key, _laBytes):
		var sec int64
		sec, err = parseIntFromBytes(val)
		if err == nil {
			item.LastAccess = time.Duration(sec) * time.Second
		}
	case bytes.Equal(key, _casBytes):
		item.CAS, err = parseUintFromBytes(val)
	case bytes.Equal(key, _fetchBytes):
		item.HitBefore = bytes.Equal(val, _yesBytes)
	case bytes.Equal(key, _clsBytes):
		item.SlabClassID, err = parseUintFromBytes(val)
	case bytes.Equal(key, _sizeBytes):
		item.Size, err = parseUintFromBytes(val)
	default:
		item.Fields = appendStatField(item.Fields, key, val)
	}
	if err != nil {
		return fmt.Errorf("invalid meta debug field %s: %w", key, ErrMalformedResponse)
	}
	return nil
}

func appendStatField(dst []DebugField, key, val []byte) []DebugField {
	n := len(dst)
	if n < cap(dst) {
		dst = dst[:n+1]
	} else {
		dst = append(dst, DebugField{})
	}
	dst[n].Key = append(dst[n].Key[:0], key...)
	dst[n].Value = append(dst[n].Value[:0], val...)
	return dst
}
