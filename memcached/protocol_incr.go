package memcached

import (
	"bytes"
	"fmt"
	"strconv"
)

// buildArithmeticCommand encodes incr/decr <key> <delta> [noreply]\r\n.
func buildArithmeticCommand(command string, key []byte, delta uint64, noReply bool) (*request, *response) {
	b := newProtocolBuilder().
		AddString(command).
		AddBytes(key).
		AddUint(delta)
	defer b.release()

	if noReply {
		b.AddBytes(_NoReplyBytes)
	}

	raw := b.AddCRLF().build()

	req := &request{
		cmd: []byte(command),
		key: key,
		raw: raw,
	}

	var resp *response
	if noReply {
		resp = buildNoReplyResponse()
	} else {
		resp = buildLimitedLineResponse(1)
	}

	return req, resp
}

// parseArithmetic reads the decimal counter from an incr/decr reply.
func parseArithmetic(line []byte) (uint64, error) {
	s := string(bytes.TrimSpace(trimCRLF(line)))
	if s == "" {
		return 0, fmt.Errorf("empty arithmetic reply: %w", ErrMalformedResponse)
	}
	return strconv.ParseUint(s, 10, 64)
}
