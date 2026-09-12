package memcached

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"time"
)

func parseMetaHeader(line []byte, item *MetaItem) (cd []byte, size uint64, hasSize bool, err error) {
	line = trimCRLF(line)
	if len(line) == 0 {
		return nil, 0, false, fmt.Errorf("invalid response: %w", ErrMalformedResponse)
	}
	end := bytes.IndexByte(line, ' ')
	cd = line
	if end >= 0 {
		cd = line[:end]
	}
	item.status = metaStatusFromCD(cd)
	if end < 0 {
		return cd, 0, false, nil
	}
	rest := line[end+1:]
	if bytes.Equal(cd, _VABytes) {
		sizeTok := rest
		rest = nil
		if sp := bytes.IndexByte(sizeTok, ' '); sp >= 0 {
			rest = sizeTok[sp+1:]
			sizeTok = sizeTok[:sp]
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
	return bindMetaKeyFromWire(item)
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
		item.TTL = time.Duration(n) * time.Second
	case 'l':
		n, err := parseUintFromBytes(value)
		if err != nil {
			return fmt.Errorf("invalid %c flag: %w", flag, ErrMalformedResponse)
		}
		if n > math.MaxInt64 {
			return fmt.Errorf("l flag exceeds int64: %w", ErrMalformedResponse)
		}
		item.LastAccess = time.Duration(n) * time.Second
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
		}
		item.Opaque = append(item.Opaque[:0], value...)
	case 'h':
		n, err := parseUintFromBytes(value)
		if err != nil {
			return fmt.Errorf("invalid %c flag: %w", flag, ErrMalformedResponse)
		}
		item.HitBefore = n == 1
	case 'k':
		item.WireKey = append(item.WireKey[:0], value...)
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

func opaqueIsCanonical(b []byte, n uint64) bool {
	var buf [20]byte
	return bytes.Equal(b, strconv.AppendUint(buf[:0], n, 10))
}

func opaqueStringIsCanonical(token string, n uint64) bool {
	var buf [20]byte
	enc := strconv.AppendUint(buf[:0], n, 10)
	if len(enc) != len(token) {
		return false
	}
	for i, c := range enc {
		if token[i] != c {
			return false
		}
	}
	return true
}

func bindMetaKeyFromWire(item *MetaItem) error {
	if item == nil || len(item.WireKey) == 0 {
		return nil
	}
	if item.KeyBase64 {
		dst := item.Key
		if sameByteArray(dst, item.WireKey) {
			dst = nil
		}
		dec, derr := base64DecodeInto(dst, item.WireKey)
		if derr != nil {
			return fmt.Errorf("invalid k/b key: %w", ErrMalformedResponse)
		}
		item.Key = dec
		return nil
	}
	if len(item.Key) == 0 {
		item.Key = item.WireKey
	}
	return nil
}
