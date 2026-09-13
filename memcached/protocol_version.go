package memcached

import (
	"bytes"
	"fmt"
)

// buildVersionCommand encodes version\r\n.
func buildVersionCommand() (*request, *response) {
	req := buildRequest([]byte("version"), nil, []byte("version\r\n"))
	resp := buildLimitedLineResponse(1)
	return req, resp
}

// parseVersionLine reads the string after VERSION.
func parseVersionLine(lines [][]byte) (string, error) {
	if len(lines) == 0 {
		return "", fmt.Errorf("unexpected version: %w", ErrMalformedResponse)
	}
	line := trimCRLF(lines[0])
	if !bytes.HasPrefix(line, _VersionBytes) {
		return "", fmt.Errorf("%s: %w", string(line), ErrMalformedResponse)
	}
	return string(bytes.TrimSpace(line[len(_VersionBytes):])), nil
}
