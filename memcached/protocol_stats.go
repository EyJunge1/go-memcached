package memcached

import (
	"bytes"
	"fmt"
)

var (
	_STATSpaceBytes   = []byte("STAT ")
	_PREFIXBytes      = []byte("PREFIX")
	_PREFIXSpaceBytes = []byte("PREFIX ")
)

// cutStatLine splits a STAT <key> <value> line.
func cutStatLine(line []byte) (key, val []byte, ok bool) {
	if !bytes.HasPrefix(line, _STATSpaceBytes) {
		return nil, nil, false
	}
	rest := line[len(_STATSpaceBytes):]
	name, value, found := bytes.Cut(rest, _SpaceBytes)
	if !found || len(name) == 0 {
		return nil, nil, false
	}
	return name, value, true
}

func skipStatsNoise(trimmed []byte) bool {
	return bytes.Equal(trimmed, _EndBytes) ||
		bytes.Equal(trimmed, _RESETBytes) ||
		bytes.Equal(trimmed, _OKBytes) ||
		bytes.Equal(trimmed, _PREFIXBytes) ||
		bytes.HasPrefix(trimmed, _PREFIXSpaceBytes)
}

func appendStat(dst []Stat, name, val []byte) []Stat {
	n := len(dst)
	if n < cap(dst) {
		dst = dst[:n+1]
	} else {
		dst = append(dst, Stat{})
	}
	dst[n].Name = append(dst[n].Name[:0], name...)
	dst[n].Value = append(dst[n].Value[:0], val...)
	return dst
}

// buildStatsCommand encodes stats [subcommand]\r\n and reads until END.
func buildStatsCommand(subCommand string) (*request, *response) {
	b := newProtocolBuilder().
		AddString("stats")

	defer b.release()

	if subCommand != "" {
		b.AddString(subCommand)
	}

	req := b.AddCRLF().request(_StatsCmdBytes, nil)
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
	req := b.AddCRLF().request(_StatsCmdBytes, nil)
	if mode == "dump" {
		return req, buildSpecEndLineResponse(_EndCRLFBytes, 64)
	}
	return req, buildLimitedLineResponse(1)
}

func mapStatsReply(resp *response) error {
	if resp.statsFilled {
		return nil
	}
	return parseStatsInto(resp.rawLines, resp.statsDst)
}

func mapStatsDetailDump(resp *response) error {
	if resp.statsFilled {
		return nil
	}
	return parseStatsDetailInto(resp.rawLines, resp.statsDst)
}

func consumeStatsLine(dst *[]Stat, line []byte, detail bool) error {
	if dst == nil {
		return fmt.Errorf("stats dst: %w", ErrInvalidArgument)
	}
	trimmed := trimCRLF(line)
	if bytes.Equal(trimmed, _EndBytes) ||
		bytes.Equal(trimmed, _RESETBytes) ||
		bytes.Equal(trimmed, _OKBytes) ||
		bytes.Equal(trimmed, _PREFIXBytes) {
		return nil
	}
	if bytes.HasPrefix(trimmed, _PREFIXSpaceBytes) {
		if !detail {
			return nil
		}
		rest := trimmed[len(_PREFIXSpaceBytes):]
		key, val, ok := bytes.Cut(rest, _SpaceBytes)
		if !ok {
			*dst = appendStat(*dst, rest, nil)
			return nil
		}
		*dst = appendStat(*dst, key, val)
		return nil
	}
	if key, val, ok := cutStatLine(trimmed); ok {
		*dst = appendStat(*dst, key, val)
		return nil
	}
	hit := mapWireLine(line, wireStats)
	if hit.Class == wireFault {
		return hit.Err
	}
	return fmt.Errorf("%s: %w", trimmed, ErrMalformedResponse)
}

// parseStatsDetailInto copies PREFIX and STAT lines from a detail dump into dst.
func parseStatsDetailInto(lines [][]byte, dst *[]Stat) error {
	if dst == nil {
		return fmt.Errorf("stats dst: %w", ErrInvalidArgument)
	}
	acc := (*dst)[:0]
	for _, line := range lines {
		trimmed := trimCRLF(line)
		if bytes.Equal(trimmed, _EndBytes) {
			continue
		}
		if bytes.HasPrefix(trimmed, _PREFIXSpaceBytes) {
			rest := trimmed[len(_PREFIXSpaceBytes):]
			key, val, ok := bytes.Cut(rest, _SpaceBytes)
			if !ok {
				acc = appendStat(acc, rest, nil)
				continue
			}
			acc = appendStat(acc, key, val)
			continue
		}
		if key, val, ok := cutStatLine(trimmed); ok {
			acc = appendStat(acc, key, val)
			continue
		}
		hit := mapWireLine(line, wireStats)
		if hit.Class == wireFault {
			*dst = acc
			return hit.Err
		}
		*dst = acc
		return fmt.Errorf("%s: %w", trimmed, ErrMalformedResponse)
	}
	*dst = acc
	return nil
}

// parseStatsInto copies STAT lines into dst, skipping END, RESET, OK, and PREFIX.
func parseStatsInto(lines [][]byte, dst *[]Stat) error {
	if dst == nil {
		return fmt.Errorf("stats dst: %w", ErrInvalidArgument)
	}
	acc := (*dst)[:0]
	for _, line := range lines {
		trimmed := trimCRLF(line)
		if skipStatsNoise(trimmed) {
			continue
		}
		if key, val, ok := cutStatLine(trimmed); ok {
			acc = appendStat(acc, key, val)
			continue
		}
		hit := mapWireLine(line, wireStats)
		if hit.Class == wireFault {
			*dst = acc
			return hit.Err
		}
		*dst = acc
		return fmt.Errorf("%s: %w", trimmed, ErrMalformedResponse)
	}
	*dst = acc
	return nil
}
