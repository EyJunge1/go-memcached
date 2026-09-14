package memcached

import (
	"bytes"
	"fmt"
)

// buildVersionCommand encodes version\r\n.
func buildVersionCommand() (*request, *response) {
	req := buildRequest(_VersionCmdBytes, nil, _VersionCRLFBytes)
	resp := buildLimitedLineResponse(1)
	return req, resp
}

// parseVersionInto copies the token after VERSION into dst.
func parseVersionInto(lines [][]byte, dst *[]byte) error {
	if dst == nil {
		return fmt.Errorf("version dst: %w", ErrInvalidArgument)
	}
	if len(lines) == 0 {
		*dst = (*dst)[:0]
		return fmt.Errorf("unexpected version: %w", ErrMalformedResponse)
	}
	line := trimCRLF(lines[0])
	if !bytes.HasPrefix(line, _VersionBytes) {
		*dst = (*dst)[:0]
		return fmt.Errorf("%s: %w", line, ErrMalformedResponse)
	}
	ver := bytes.TrimSpace(line[len(_VersionBytes):])
	*dst = append((*dst)[:0], ver...)
	return nil
}

func mapVersionReply(resp *response) error {
	return parseVersionInto(resp.rawLines, resp.versionDst)
}
