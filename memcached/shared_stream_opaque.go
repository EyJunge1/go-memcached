package memcached

import (
	"bytes"
	"fmt"
	"strconv"
)

// isMetaMuxCmd reports commands that participate in SharedStreams opaque demux.
func isMetaMuxCmd(cmd []byte) bool {
	return bytes.Equal(cmd, _MgBytes) ||
		bytes.Equal(cmd, _MsBytes) ||
		bytes.Equal(cmd, _MdBytes) ||
		bytes.Equal(cmd, _MaBytes)
}

func (j *streamJob) isMetaMux() bool {
	if j == nil {
		return false
	}
	if j.resp != nil && j.resp.metaPipeDst != nil {
		return true
	}
	if j.run != nil || j.req == nil {
		return false
	}
	return isMetaMuxCmd(j.req.cmd)
}

func (j *streamJob) isQuietMeta() bool {
	if j == nil || j.resp == nil {
		return false
	}
	if j.resp.metaPipeDst != nil {
		return true
	}
	return j.resp.endIndicator == _endIndicatorQuietBarrier
}

// stripJobQuietNoop removes a trailing mn from a single-op meta frame so the
// stream writer can emit one barrier per write window.
func stripJobQuietNoop(j *streamJob) {
	if j == nil || j.req == nil || j.resp == nil || j.resp.metaPipeDst != nil {
		return
	}
	if j.resp.endIndicator != _endIndicatorQuietBarrier {
		return
	}
	req := j.req
	if bytes.Equal(req.tail, _CRLFMnCRLFBytes) {
		req.tail = _CRLFBytes
		return
	}
	if bytes.HasSuffix(req.raw, _MnCRLFBytes) {
		req.raw = req.raw[:len(req.raw)-len(_MnCRLFBytes)]
		if req.rawBox == nil {
			return
		}
		box := *req.rawBox
		if box == nil || len(box) < len(req.raw) {
			return
		}
		box = box[:len(req.raw)]
		*req.rawBox = box
		req.raw = box
	}
}

func scanMetaRequestOpaque(line []byte) (num uint64, hasNum bool, tok []byte, found bool) {
	line = trimCRLF(line)
	// Skip verb and key: "mg <key> <flags>*" / "ms <key> <len> <flags>*"
	rest := line
	for skip := 0; skip < 2; skip++ {
		sp := bytes.IndexByte(rest, ' ')
		if sp < 0 {
			return 0, false, nil, false
		}
		rest = rest[sp+1:]
	}
	// ms has an extra length token before flags.
	if bytes.HasPrefix(line, _MsBytes) {
		sp := bytes.IndexByte(rest, ' ')
		if sp < 0 {
			return 0, false, nil, false
		}
		rest = rest[sp+1:]
	}
	start := 0
	for i := 0; i <= len(rest); i++ {
		if i < len(rest) && rest[i] != ' ' {
			continue
		}
		if i > start && rest[start] == 'O' {
			val := rest[start+1 : i]
			if n, err := parseUintFromBytes(val); err == nil && opaqueIsCanonical(val, n) {
				return n, true, nil, true
			}
			return 0, false, val, true
		}
		start = i + 1
	}
	return 0, false, nil, false
}

func injectMetaRequestOpaque(raw []byte, tok string) ([]byte, error) {
	if len(tok) == 0 {
		return nil, fmt.Errorf("empty opaque: %w", ErrInvalidArgument)
	}
	if err := validOpaqueToken(tok); err != nil {
		return nil, err
	}
	if !bytes.HasSuffix(raw, _CRLFBytes) {
		return nil, fmt.Errorf("meta line missing CRLF: %w", ErrMalformedResponse)
	}
	base := raw[:len(raw)-len(_CRLFBytes)]
	out := make([]byte, 0, len(base)+2+len(tok)+len(_CRLFBytes))
	out = append(out, base...)
	out = append(out, ' ', 'O')
	out = append(out, tok...)
	out = append(out, _CRLFBytes...)
	return out, nil
}

// injectMetaRequestOpaqueUint appends " O<n>\r\n" using strconv.AppendUint.
// When req.rawBox has capacity, the opaque is written in place (no string alloc).
func injectMetaRequestOpaqueUint(req *request, n uint64) error {
	if req == nil {
		return fmt.Errorf("missing meta request: %w", ErrInvalidArgument)
	}
	raw := req.raw
	if !bytes.HasSuffix(raw, _CRLFBytes) {
		return fmt.Errorf("meta line missing CRLF: %w", ErrMalformedResponse)
	}
	baseLen := len(raw) - len(_CRLFBytes)
	var numBuf [20]byte
	enc := strconv.AppendUint(numBuf[:0], n, 10)
	need := baseLen + 2 + len(enc) + len(_CRLFBytes)

	if req.rawBox != nil {
		box := *req.rawBox
		if box != nil && cap(box) >= need {
			box = box[:baseLen]
			box = append(box, ' ', 'O')
			box = append(box, enc...)
			box = append(box, _CRLFBytes...)
			*req.rawBox = box
			req.raw = box
			return nil
		}
	}

	out := make([]byte, 0, need)
	out = append(out, raw[:baseLen]...)
	out = append(out, ' ', 'O')
	out = append(out, enc...)
	out = append(out, _CRLFBytes...)
	req.raw = out
	req.rawBox = nil
	req.rawPut = nil
	return nil
}

// ensureJobOpaque assigns or records the O token for a muxed meta single-op.
func (s *sharedStream) ensureJobOpaque(j *streamJob) (num uint64, hasNum bool, tok string, err error) {
	if j == nil || j.req == nil {
		return 0, false, "", fmt.Errorf("missing meta job: %w", ErrInvalidArgument)
	}
	num, hasNum, tokBytes, found := scanMetaRequestOpaque(j.req.raw)
	if found {
		if hasNum {
			return num, true, "", nil
		}
		return 0, false, string(tokBytes), nil
	}
	seq := s.c.opaqueSeq.Add(1)
	if err = injectMetaRequestOpaqueUint(j.req, seq); err != nil {
		return 0, false, "", err
	}
	return seq, true, "", nil
}
