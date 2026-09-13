package memcached

import (
	"bytes"
	"fmt"
)

// cutStatLine splits a STAT <key> <value> line.
func cutStatLine(line []byte) (key, val string, ok bool) {
	const prefix = "STAT "
	if !bytes.HasPrefix(line, []byte(prefix)) {
		return "", "", false
	}
	rest := line[len(prefix):]
	name, value, found := bytes.Cut(rest, []byte{' '})
	if !found || len(name) == 0 {
		return "", "", false
	}
	return string(name), string(value), true
}

// buildStatsCommand encodes stats [subcommand]\r\n and reads until END.
func buildStatsCommand(subCommand string) (*request, *response) {
	b := newProtocolBuilder().
		AddString("stats")

	defer b.release()

	if subCommand != "" {
		b.AddString(subCommand)
	}

	req := b.AddCRLF().request([]byte("stats"), nil)
	if subCommand == "reset" {
		return req, buildLimitedLineResponse(1)
	}
	resp := buildSpecEndLineResponse(_EndCRLFBytes, 64)

	return req, resp
}

// buildStatsDetailCommand encodes stats detail <mode>.
func buildStatsDetailCommand(mode string) (*request, *response) {
	b := newProtocolBuilder().AddString("stats").AddString("detail").AddString(mode)
	defer b.release()
	req := b.AddCRLF().request([]byte("stats"), nil)
	if mode == "dump" {
		return req, buildSpecEndLineResponse(_EndCRLFBytes, 64)
	}
	return req, buildLimitedLineResponse(1)
}

// parseStatsDetailDump maps PREFIX and STAT lines from a detail dump.
func parseStatsDetailDump(lines [][]byte) (map[string]string, error) {
	out := make(map[string]string, len(lines))
	for _, line := range lines {
		trimmed := trimCRLF(line)
		if bytes.Equal(trimmed, []byte("END")) {
			continue
		}
		if bytes.HasPrefix(trimmed, []byte("PREFIX ")) {
			rest := trimmed[len("PREFIX "):]
			key, val, ok := bytes.Cut(rest, []byte(" "))
			if !ok {
				out[string(rest)] = ""
				continue
			}
			out[string(key)] = string(val)
			continue
		}
		hit := mapWireLine(line, wireStats)
		if hit.Class == wireFault {
			return nil, hit.Err
		}
		key, val, ok := cutStatLine(trimmed)
		if !ok {
			return nil, fmt.Errorf("%s: %w", trimmed, ErrMalformedResponse)
		}
		out[key] = val
	}
	return out, nil
}

// parseStatsMap maps STAT lines, skipping END, RESET, OK, and PREFIX.
func parseStatsMap(lines [][]byte) (map[string]string, error) {
	out := make(map[string]string, len(lines))
	for _, line := range lines {
		trimmed := trimCRLF(line)
		if bytes.Equal(trimmed, []byte("END")) || bytes.Equal(trimmed, []byte("RESET")) ||
			bytes.Equal(trimmed, []byte("OK")) || bytes.Equal(trimmed, []byte("PREFIX")) ||
			bytes.HasPrefix(trimmed, []byte("PREFIX ")) {
			continue
		}
		hit := mapWireLine(line, wireStats)
		if hit.Class == wireFault {
			return nil, hit.Err
		}
		key, val, ok := cutStatLine(trimmed)
		if !ok {
			return nil, fmt.Errorf("%s: %w", trimmed, ErrMalformedResponse)
		}
		out[key] = val
	}
	return out, nil
}
