package memcached

import (
	"fmt"
)

// buildArithmeticCommand encodes incr/decr <key> <delta> [noreply]\r\n.
func buildArithmeticCommand(command string, key []byte, delta uint64, noReply bool) (*request, *response) {
	verb := verbBytes(command)
	b := newProtocolBuilder().
		AddBytes(verb).
		AddBytes(key).
		AddUint(delta)
	defer b.release()

	if noReply {
		b.AddBytes(_NoReplyBytes)
	}

	req := b.AddCRLF().request(verb, key)

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
	line = trimSpaceBytes(trimCRLF(line))
	if len(line) == 0 {
		return 0, fmt.Errorf("empty arithmetic reply: %w", ErrMalformedResponse)
	}
	return parseUintFromBytes(line)
}

func trimSpaceBytes(b []byte) []byte {
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\t') {
		b = b[1:]
	}
	for n := len(b); n > 0 && (b[n-1] == ' ' || b[n-1] == '\t'); n-- {
		b = b[:n-1]
	}
	return b
}
