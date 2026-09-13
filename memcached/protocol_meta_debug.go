package memcached

import (
	"bytes"
	"fmt"
	"strconv"
)

// MetaDebugOption is used to set options for MetaDebug command.
type MetaDebugOption func(*metaDebugFlags)

type metaDebugFlags struct {
	b bool // b: interpret key as base64 encoded binary value
}

// MetaDebugFlagBinaryKey sets the flag to interpret key as base64 encoded binary value.
func MetaDebugFlagBinaryKey() MetaDebugOption {
	return func(flags *metaDebugFlags) { flags.b = true }
}

// me <key> <flag>\r\n
func buildMetaDebugCommand(key []byte, flags *metaDebugFlags) (*request, *response) {
	if flags.b {
		key = base64Encode(key)
	}

	b := newProtocolBuilder().
		AddString("me").
		AddBytes(key)
	defer b.release()

	b.AddFlagBool("b", flags.b).AddCRLF()

	req := buildRequest([]byte("me"), key, b.build())
	resp := buildLimitedLineResponse(1)

	return req, resp
}

// parseMetaItemDebug parses the response from memcached server. It could be:
// success: ME foo exp=-1 la=2 cas=18 fetch=no cls=1 size=65\r\n
// failed:  EN\r\n
func parseMetaItemDebug(lines [][]byte, item *MetaItemDebug) error {
	if len(lines) != 1 {
		return fmt.Errorf("invalid response: %w", ErrMalformedResponse)
	}

	parts := bytes.Split(trimCRLF(lines[0]), _SpaceBytes)
	if len(parts) < 1 {
		return fmt.Errorf("invalid response: %w", ErrMalformedResponse)
	}

	const (
		CDIndex  = 0
		KeyIndex = 1
	)

	cd := parts[CDIndex]
	switch string(cd) {
	case "EN":
		return ErrNotFound
	case "ME":
		// success
	default:
		return fmt.Errorf("unexpected cd<=%s>: %w", string(cd), ErrMalformedResponse)
	}

	// parse key
	// item.Key = parts[KeyIndex]
	for i := KeyIndex + 1; i < len(parts); i++ {
		kv := bytes.Split(parts[i], []byte("="))
		if len(kv) != 2 {
			continue
		}

		switch string(kv[0]) {
		case "exp":
			item.TTL, _ = strconv.ParseInt(string(kv[1]), 10, 64)
		case "la":
			item.LastAccessTime, _ = strconv.ParseInt(string(kv[1]), 10, 64)
		case "cas":
			item.CAS, _ = strconv.ParseUint(string(kv[1]), 10, 64)
		case "fetch":
			item.HitBefore = string(kv[1]) == "yes"
		case "cls":
			item.SlabClassID, _ = strconv.ParseUint(string(kv[1]), 10, 32)
		case "size":
			item.Size, _ = strconv.ParseUint(string(kv[1]), 10, 64)
		}
	}

	return nil
}
