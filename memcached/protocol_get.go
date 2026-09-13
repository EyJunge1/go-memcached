package memcached

import (
	"bytes"
	"fmt"
	"math"
)

// buildGetsCommand constructs gets command.
// get/gets <key>*\r\n
func buildGetsCommand(command string, keys ...[]byte) (*request, *response) {
	b := newProtocolBuilder().
		AddString(command)
	defer b.release()

	for _, key := range keys {
		b.AddBytes(key)
	}
	b.AddCRLF()

	var pickKey []byte
	if len(keys) == 1 {
		pickKey = keys[0]
	}
	req := buildRequest([]byte(command), pickKey, b.build())
	resp := buildSpecEndLineResponse(_EndCRLFBytes, len(keys)*2+1)

	return req, resp
}

// buildGetAndTouchCommand constructs get and touch command.
// gat/gats <exptime> <key>*\r\n
func buildGetAndTouchesCommand(command string, expiry Exptime, keys ...[]byte) (*request, *response) {
	b := newProtocolBuilder().
		AddString(command).
		AddInt64(expiryToken(expiry))
	defer b.release()

	for _, key := range keys {
		b.AddBytes(key)
	}

	b.AddCRLF()

	var pickKey []byte
	if len(keys) == 1 {
		pickKey = keys[0]
	}
	req := buildRequest([]byte(command), pickKey, b.build())
	resp := buildSpecEndLineResponse(_EndCRLFBytes, len(keys)*2+1)

	return req, resp
}

// parseValueItems parses the response from memcached server, the response
// is a list of items, each item is a key-value pair.
//
// For example:
// VALUE <key> <flags> <bytes> <cas unique>\r\n
// <data block>\r\n
// VALUE <key> <flags> <bytes> <cas unique>\r\n
// <data block>\r\n
// ...
// END\r\n
func parseValueItems(lines [][]byte, withoutEndLine, withCAS bool) (_ []*Item, err error) {
	n := len(lines)
	if withoutEndLine && n%2 != 0 {
		// n must be even
		return nil, fmt.Errorf("want times of 2 lines, got %d: %w", n, ErrMalformedResponse)
	}
	if !withoutEndLine && n%2 == 0 {
		// n must be odd
		return nil, fmt.Errorf("want odd lines, got %d: %w", n, ErrMalformedResponse)
	}

	var (
		rn      = n
		items   = make([]*Item, 0, (n/2)+1) // pre-alloc to avoid memory allocation
		dataLen uint64
	)

	if !withoutEndLine {
		// avoid iterate the last line "END\r\n"
		rn = n - 1
	}

	for i := 0; i < rn; i += 2 {
		line := trimCRLF(lines[i])
		if !bytes.HasPrefix(line, []byte("VALUE ")) {
			return nil, fmt.Errorf("unexpected retrieval line: %w", ErrMalformedResponse)
		}

		item := &Item{}
		dataLen, err = parseValueLine(line, item, withCAS)
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

// dataBlockValue returns the payload of a length-framed VALUE/VA body.
// The reader stores size+CRLF; the value is the declared size, not a trim of the
// last CRLF (a value may itself end in CR/LF).
func dataBlockValue(raw []byte, size uint64) ([]byte, error) {
	if uint64(len(raw)) == size+2 && bytes.HasSuffix(raw, _CRLFBytes) {
		return raw[:size], nil
	}
	return nil, fmt.Errorf("data block length mismatch: %w", ErrMalformedResponse)
}

// parseValueLine extract item from VALUE line, like following:
// VALUE <key> <flags> <bytes> <cas unique> => Item{key, flags, cas}
//
// if withCas is false, VALUE line is:
// VALUE <key> <flags> <bytes> => Item{key, flags, 0}
func parseValueLine(line []byte, item *Item, withCas bool) (dataLen uint64, err error) {
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

		// another field starts from fieldStart to i, or the last field
		// the 'i' is the index of space or the last byte.
		switch nField {
		case keyIndex:
			item.Key = append(item.Key[:0], line[fieldStart:i]...)
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

// parseUintFromBytes parses slice of bytes to uint64.
//
// For example:
// []byte("1234567890") -> 1234567890
//
// If the slice of bytes is empty, it returns 0.
//
// If the slice of bytes contains non-digit characters, it returns an error.
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
