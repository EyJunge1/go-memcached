package memcached

import (
	"fmt"
)

// buildArithmeticCommand encodes incr/decr <key> <delta> [extra…] [noreply]\r\n.
func buildArithmeticCommand(
	command string, key []byte, delta uint64, extra []string, noReply bool,
) (*request, *response, error) {
	verb := verbBytes(command)
	b := newProtocolBuilder().
		AddBytes(verb).
		AddBytes(key).
		AddUint(delta)
	defer b.release()

	if err := b.appendTextTrail(extra, noReply); err != nil {
		return nil, nil, err
	}
	req := b.AddCRLF().request(verb, key)

	resp := buildLimitedLineResponse(1)
	if noReply {
		resp = buildNoReplyResponse()
	}

	return req, resp, nil
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
	if b == nil {
		return _emptyBytes
	}
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\t') {
		b = b[1:]
	}
	for n := len(b); n > 0 && (b[n-1] == ' ' || b[n-1] == '\t'); n-- {
		b = b[:n-1]
	}
	return b
}
