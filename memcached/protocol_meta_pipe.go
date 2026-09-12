package memcached

import (
	"errors"
	"fmt"
	"io"
)

func (resp *response) fillMetaPipe(rr protocolReader, header []byte) error {
	if resp == nil || resp.metaPipeDst == nil {
		return fmt.Errorf("missing meta pipeline dest: %w", ErrMalformedResponse)
	}
	resp.metaSawReply = true
	pos, ok := matchMetaPipelineOpaque(header, resp.metaPipeGroup, resp.metaPipeIndex)
	if !ok {
		_, hasNum, tok := scanMetaOpaque(header)
		if hasNum || len(tok) > 0 {
			return fmt.Errorf("unknown meta opaque response: %w", ErrMalformedResponse)
		}
		return fmt.Errorf("missing meta opaque response: %w", ErrMalformedResponse)
	}
	prep := &resp.metaPipeGroup[pos]
	idx := prep.idx
	if metaPipeOpaqueSeen(resp, pos) {
		return fmt.Errorf("duplicate meta opaque response: %w", ErrMalformedResponse)
	}
	markMetaPipeOpaqueSeen(resp, pos)

	item := &resp.metaPipeDst[idx].Item
	_, size, hasSize, err := parseMetaHeader(header, item)
	if err != nil {
		return err
	}
	if item.status == MetaStatusHD {
		item.gotHD = true
	}
	if item.status == MetaStatusVA {
		if !hasSize {
			return fmt.Errorf("missing value size: %w", ErrMalformedResponse)
		}
		n, err := cappedValueSize(size, resp.maxValueBytes)
		if err != nil {
			return err
		}
		item.Value = takeValueBuffer(item.Value, n)
		if err = readFramedValueInto(rr, item.Value); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return fmt.Errorf("short data block: %w", ErrMalformedResponse)
			}
			return fmt.Errorf("read data block: %w", err)
		}
	}
	if prep.hasGet {
		applyCheckCAS(item, &prep.getFlags)
	}
	finishMetaPipelineDest(resp.metaPipeC, resp.metaPipeDst, idx, prep)
	return nil
}

func metaPipeOpaqueSeen(resp *response, pos int) bool {
	byteIdx := pos >> 3
	if byteIdx >= len(resp.metaPipeSeen) {
		return false
	}
	return resp.metaPipeSeen[byteIdx]&(1<<uint(pos&7)) != 0
}

func markMetaPipeOpaqueSeen(resp *response, pos int) {
	need := (pos >> 3) + 1
	if len(resp.metaPipeSeen) < need {
		grown := make([]byte, need)
		copy(grown, resp.metaPipeSeen)
		resp.metaPipeSeen = grown
	}
	resp.metaPipeSeen[pos>>3] |= 1 << uint(pos&7)
}

func resetMetaPipeSeen(resp *response, n int) {
	if resp == nil {
		return
	}
	need := (n + 7) >> 3
	if cap(resp.metaPipeSeen) >= need {
		resp.metaPipeSeen = resp.metaPipeSeen[:need]
		clear(resp.metaPipeSeen)
		return
	}
	resp.metaPipeSeen = make([]byte, need)
}
