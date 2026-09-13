package memcached

import (
	"bytes"
	"fmt"
	"math"
)

func pickKey(keys [][]byte) []byte {
	if len(keys) == 1 {
		return keys[0]
	}
	return nil
}

// encodeGetsCommand encodes get/gets <key>*\r\n.
func encodeGetsCommand(command string, keys ...[]byte) *request {
	b := newProtocolBuilder().
		AddString(command)
	defer b.release()

	for _, key := range keys {
		b.AddBytes(key)
	}
	b.AddCRLF()
	return b.request(verbBytes(command), pickKey(keys))
}

// buildGetsCommand encodes get/gets and reads until END (auth and tests).
func buildGetsCommand(command string, keys ...[]byte) (*request, *response) {
	req := encodeGetsCommand(command, keys...)
	resp := buildSpecEndLineResponse(_EndCRLFBytes, len(keys)*2+1)
	return req, resp
}

// encodeGetAndTouchesCommand encodes gat/gats <exptime> <key>*\r\n.
func encodeGetAndTouchesCommand(command string, expiry Exptime, keys ...[]byte) *request {
	b := newProtocolBuilder().
		AddString(command).
		AddInt64(expiryToken(expiry))
	defer b.release()

	for _, key := range keys {
		b.AddBytes(key)
	}
	b.AddCRLF()
	return b.request(verbBytes(command), pickKey(keys))
}

// buildGetAndTouchesCommand encodes gat/gats and reads until END.
func buildGetAndTouchesCommand(command string, expiry Exptime, keys ...[]byte) (*request, *response) {
	req := encodeGetAndTouchesCommand(command, expiry, keys...)
	resp := buildSpecEndLineResponse(_EndCRLFBytes, len(keys)*2+1)
	return req, resp
}

// parseValueItems reads VALUE/data pairs, optionally ending with END.
func parseValueItems(lines [][]byte, withoutEndLine, withCAS bool) (_ []Item, err error) {
	n := len(lines)
	if withoutEndLine && n%2 != 0 {
		return nil, fmt.Errorf("want times of 2 lines, got %d: %w", n, ErrMalformedResponse)
	}
	if !withoutEndLine && n%2 == 0 {
		return nil, fmt.Errorf("want odd lines, got %d: %w", n, ErrMalformedResponse)
	}

	var (
		rn    = n
		items = make([]Item, 0, (n/2)+1)
	)

	if !withoutEndLine {
		rn = n - 1
	}

	for i := 0; i < rn; i += 2 {
		line := trimCRLF(lines[i])
		if !bytes.HasPrefix(line, _ValueSpaceBytes) {
			return nil, fmt.Errorf("unexpected retrieval line: %w", ErrMalformedResponse)
		}

		var item Item
		dataLen, err := parseValueLine(line, &item, withCAS)
		if err != nil {
			return nil, err
		}

		if i+1 >= n {
			return nil, fmt.Errorf("missing data block: %w", ErrMalformedResponse)
		}
		item.Value, err = dataBlockValue(lines[i+1], dataLen) //nolint:gosec // G602: i+1 is bounds-checked above
		if err != nil {
			return nil, err
		}

		items = append(items, item)
	}

	return items, nil
}

// dataBlockValue slices a length-framed VALUE/VA body to the declared size, not by trimming CRLF.
func dataBlockValue(raw []byte, size uint64) ([]byte, error) {
	if uint64(len(raw)) == size+2 && bytes.HasSuffix(raw, _CRLFBytes) {
		return raw[:size], nil
	}
	return nil, fmt.Errorf("data block length mismatch: %w", ErrMalformedResponse)
}

// parseValueLine fills item from VALUE <key> <flags> <bytes> [cas] and owns the key.
func parseValueLine(line []byte, item *Item, withCas bool) (dataLen uint64, err error) {
	return parseValueLineInto(line, item, withCas, true)
}

// parseValueLineBorrowed fills item while leaving Key backed by line.
// Callers must copy Key before their next read invalidates line.
func parseValueLineBorrowed(line []byte, item *Item, withCas bool) (dataLen uint64, err error) {
	return parseValueLineInto(line, item, withCas, false)
}

func parseValueLineInto(line []byte, item *Item, withCas, copyKey bool) (dataLen uint64, err error) {
	const (
		keyIndex     = 1
		flagsIndex   = 2
		dataLenIndex = 3
		casIndex     = 4
	)

	n := len(line)
	start := len(_ValueBytes)
	fieldStart := start
	nField := 0

	for i := start; i < n; i++ {
		if nField > 5 || (!withCas && nField > 4) {
			return 0, fmt.Errorf("invalid VALUE line: %w", ErrMalformedResponse)
		}

		if line[i] != ' ' && i != n-1 {
			continue
		}

		switch nField {
		case keyIndex:
			if copyKey {
				item.Key = append(item.Key[:0], line[fieldStart:i]...)
			} else {
				item.Key = line[fieldStart:i]
			}
		case flagsIndex:
			flags, err := parseUintFromBytes(line[fieldStart:i])
			if err != nil {
				return 0, fmt.Errorf("invalid flags: %w", ErrMalformedResponse)
			}
			if flags > math.MaxUint32 {
				return 0, fmt.Errorf("flags exceed uint32: %w", ErrMalformedResponse)
			}
			item.Flags = uint32(flags)
		case dataLenIndex:
			si := i
			if i == n-1 {
				si = i + 1
			}
			dataLen, err = parseUintFromBytes(line[fieldStart:si])
			if err != nil {
				return 0, fmt.Errorf("invalid data length: %w", ErrMalformedResponse)
			}
		case casIndex:
			si := i
			if i == n-1 {
				si = i + 1
			}
			item.CAS, err = parseUintFromBytes(line[fieldStart:si])
			if err != nil {
				return 0, fmt.Errorf("invalid CAS: %w", ErrMalformedResponse)
			}
		}

		fieldStart = i + 1
		nField++
	}

	if len(item.Key) == 0 {
		return 0, fmt.Errorf("empty VALUE key: %w", ErrMalformedResponse)
	}
	if withCas && nField < 5 {
		return 0, fmt.Errorf("missing CAS field: %w", ErrMalformedResponse)
	}

	return dataLen, nil
}

// parseValueDataLen reads only the <bytes> field from a VALUE header.
func parseValueDataLen(line []byte) (uint64, error) {
	const dataLenIndex = 3

	n := len(line)
	start := len(_ValueBytes)
	fieldStart := start
	nField := 0
	var dataLen uint64

	for i := start; i < n; i++ {
		if nField > 5 {
			return 0, fmt.Errorf("invalid VALUE line: %w", ErrMalformedResponse)
		}
		if line[i] != ' ' && i != n-1 {
			continue
		}
		if nField == dataLenIndex {
			si := i
			if i == n-1 {
				si = i + 1
			}
			var err error
			dataLen, err = parseUintFromBytes(line[fieldStart:si])
			if err != nil {
				return 0, fmt.Errorf("invalid data length: %w", ErrMalformedResponse)
			}
			return dataLen, nil
		}
		fieldStart = i + 1
		nField++
	}
	return 0, fmt.Errorf("invalid VALUE line: %w", ErrMalformedResponse)
}

// parseUintFromBytes decodes an ASCII unsigned integer. Empty input is 0.
func parseUintFromBytes(bs []byte) (uint64, error) {
	if len(bs) == 0 {
		return 0, nil
	}

	r := uint64(0)
	for _, b := range bs {
		if b < '0' || b > '9' {
			return 0, fmt.Errorf("invalid uint number: %w", ErrMalformedResponse)
		}

		digit := uint64(b - '0')
		if r > (math.MaxUint64-digit)/10 {
			return 0, fmt.Errorf("uint number overflows uint64: %w", ErrMalformedResponse)
		}
		r = r*10 + digit
	}

	return r, nil
}
