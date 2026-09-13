package memcached

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
)

type metaDebugFlags struct {
	b bool // b: interpret key as base64 encoded binary value
	P string
	L string
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

	b.AddFlagBool("b", flags.b)
	b.AddFlagString("P", flags.P)
	b.AddFlagString("L", flags.L)
	b.AddCRLF()

	req := buildRequest([]byte("me"), key, b.build())
	resp := buildLimitedLineResponse(1)

	return req, resp
}

// parseMetaItemDebug parses the response from memcached server. It could be:
// success: ME foo exp=-1 la=2 cas=18 fetch=no cls=1 size=65\r\n
// failed:  EN\r\n
func parseMetaItemDebug(lines [][]byte, item *MetaItemDebug, binaryKey bool) error {
	if len(lines) != 1 {
		return fmt.Errorf("invalid response: %w", ErrMalformedResponse)
	}

	hit := mapWireLine(lines[0], wireMeta)
	if hit.Class == wireFault {
		if errors.Is(hit.Err, ErrNotFound) {
			item.Status = hit.Status
			return ErrNotFound
		}
		return hit.Err
	}

	parts := bytes.Fields(trimCRLF(lines[0]))
	if len(parts) < 1 {
		return fmt.Errorf("invalid response: %w", ErrMalformedResponse)
	}

	const (
		CDIndex  = 0
		KeyIndex = 1
	)

	cd := parts[CDIndex]
	item.Status = metaStatusFromCD(cd)
	switch string(cd) {
	case "EN":
		return ErrNotFound
	case "ME":
		// success
	default:
		return fmt.Errorf("unexpected cd<=%s>: %w", string(cd), ErrMalformedResponse)
	}

	if len(parts) > KeyIndex {
		item.WireKey = append(item.WireKey[:0], parts[KeyIndex]...)
		item.Key = item.WireKey
		item.KeyBase64 = binaryKey
		if binaryKey {
			dec, derr := base64Decode(item.WireKey)
			if derr != nil {
				return fmt.Errorf("invalid me b key: %w", ErrMalformedResponse)
			}
			item.Key = dec
		}
	}
	for i := KeyIndex + 1; i < len(parts); i++ {
		kv := bytes.Split(parts[i], []byte("="))
		if len(kv) != 2 {
			continue
		}

		key, val := string(kv[0]), string(kv[1])
		switch key {
		case "exp":
			item.TTL, _ = strconv.ParseInt(val, 10, 64)
		case "la":
			item.LastAccess, _ = strconv.ParseInt(val, 10, 64)
		case "cas":
			item.CAS, _ = strconv.ParseUint(val, 10, 64)
		case "fetch":
			item.HitBefore = val == "yes"
		case "cls":
			item.SlabClassID, _ = strconv.ParseUint(val, 10, 32)
		case "size":
			item.Size, _ = strconv.ParseUint(val, 10, 64)
		default:
			if item.Fields == nil {
				item.Fields = make(map[string]string)
			}
			item.Fields[key] = val
		}
	}

	return nil
}
