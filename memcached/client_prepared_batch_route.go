package memcached

import (
	"bytes"
	"fmt"
	"strconv"
)

func (b *PreparedBatch) refreshRoutes() error {
	for {
		clusterVersion := b.c.clusterVersion.Load()
		healthVersion := b.c.healthVersion.Load()
		addrs, picker := b.c.snapshotCluster()
		if picker == nil || len(addrs) == 0 {
			return fmt.Errorf("no servers configured: %w", ErrInvalidAddress)
		}
		addrs = b.c.liveAddrs(addrs)

		if len(b.p.ops) == 0 {
			b.releaseGroups()
			b.clusterVersion = clusterVersion
			b.healthVersion = healthVersion
			return nil
		}
		if len(addrs) == 1 {
			if clusterVersion != b.c.clusterVersion.Load() ||
				healthVersion != b.c.healthVersion.Load() {
				continue
			}
			b.releaseGroups()
			b.groups = make([]preparedBatchGroup, 0, 1)
			b.groups = append(b.groups, preparedBatchGroup{node: addrs[0], ops: b.p.ops})
			_, b.groups[0].frames = pipelineFrames(b.groups[0].ops)
			b.clusterVersion = clusterVersion
			b.healthVersion = healthVersion
			return nil
		}

		p := b.p
		if cap(p.routed) < len(p.ops) {
			p.routed = make([]int, len(p.ops))
		}
		p.routed = p.routed[:len(p.ops)]
		if cap(p.groupCounts) < len(addrs) {
			p.groupCounts = make([]int, len(addrs))
			p.groupOffsets = make([]int, len(addrs)+1)
		}
		p.groupCounts = p.groupCounts[:len(addrs)]
		p.groupOffsets = p.groupOffsets[:len(addrs)+1]
		clear(p.groupCounts)
		clear(p.groupOffsets)
		for i := range p.ops {
			nodeIndex, _, err := pickAddrIndex(picker, addrs, p.ops[i].key)
			if err != nil {
				return fmt.Errorf("pick node: %w", err)
			}
			p.routed[i] = nodeIndex
			p.groupCounts[nodeIndex]++
		}
		for i := range addrs {
			p.groupOffsets[i+1] = p.groupOffsets[i] + p.groupCounts[i]
			p.groupCounts[i] = p.groupOffsets[i]
		}
		if cap(p.groupedOps) < len(p.ops) {
			p.groupedOps = make([]pipeOp, len(p.ops))
		}
		p.groupedOps = p.groupedOps[:len(p.ops)]
		for i, group := range p.routed {
			p.groupedOps[p.groupCounts[group]] = p.ops[i]
			p.groupCounts[group]++
		}
		if clusterVersion != b.c.clusterVersion.Load() ||
			healthVersion != b.c.healthVersion.Load() {
			continue
		}
		b.releaseGroups()
		b.groups = make([]preparedBatchGroup, 0, len(addrs))
		for i, node := range addrs {
			start, end := p.groupOffsets[i], p.groupOffsets[i+1]
			if start != end {
				b.groups = append(b.groups, preparedBatchGroup{
					node: node,
					ops:  p.groupedOps[start:end],
				})
			}
		}
		for i := range b.groups {
			_, b.groups[i].frames = pipelineFrames(b.groups[i].ops)
		}
		b.clusterVersion = clusterVersion
		b.healthVersion = healthVersion
		return nil
	}
}

func (b *PreparedBatch) restoreDefaultValues() {
	for i, opIndex := range b.valueOps {
		b.p.ops[opIndex].req.body = b.defaultValues[i]
	}
}

func (b *PreparedBatch) updateValueLengths() error {
	for _, opIndex := range b.valueOps {
		req := b.p.ops[opIndex].req
		if err := patchStorageValueLength(req, len(req.body)); err != nil {
			return err
		}
	}
	return nil
}

func resetPreparedBatch(ops []pipeOp) {
	for i := range ops {
		op := &ops[i]
		if op.result != nil {
			*op.result = PipelineResult{}
		}
		if op.getDst != nil {
			resetGetResult(op.getDst)
		}
		if op.arithDst != nil {
			*op.arithDst = 0
		}
		if op.resp != nil {
			// Clear without writing nil literals (NilAway treats those as
			// flowing into later trimCRLF/parse sites). Truncate is enough;
			// reused slots overwrite on the next recv.
			op.resp.rawLines = op.resp.rawLines[:0]
			op.resp.arithValue = 0
		}
	}
}

func isPreparedValueOp(req *request) bool {
	if req == nil {
		return false
	}
	return bytes.Equal(req.cmd, _SetBytes) ||
		bytes.Equal(req.cmd, _AddBytes) ||
		bytes.Equal(req.cmd, _ReplaceBytes) ||
		bytes.Equal(req.cmd, _AppendBytes) ||
		bytes.Equal(req.cmd, _PrependBytes) ||
		bytes.Equal(req.cmd, _CasBytes)
}

// patchStorageValueLength updates the ASCII bytes field without rebuilding the
// command. Prepared request buffers normally have enough spare capacity even
// when the decimal field grows.
func patchStorageValueLength(req *request, valueLen int) error {
	if req == nil || len(req.raw) < 3 || !bytes.HasSuffix(req.raw, _CRLFBytes) {
		return fmt.Errorf("invalid prepared storage frame: %w", ErrInvalidArgument)
	}
	tokenEnd := len(req.raw) - len(_CRLFBytes)
	if bytes.Equal(req.cmd, _CasBytes) {
		casStart := bytes.LastIndexByte(req.raw[:tokenEnd], _SpaceByte)
		if casStart < 0 {
			return fmt.Errorf("invalid prepared cas frame: %w", ErrInvalidArgument)
		}
		tokenEnd = casStart
	}
	tokenStart := bytes.LastIndexByte(req.raw[:tokenEnd], _SpaceByte) + 1
	if tokenStart <= 0 || tokenStart >= tokenEnd {
		return fmt.Errorf("invalid prepared storage length: %w", ErrInvalidArgument)
	}
	var digits [20]byte
	encoded := strconv.AppendInt(digits[:0], int64(valueLen), 10)
	oldLen := tokenEnd - tokenStart
	newTotal := len(req.raw) + len(encoded) - oldLen
	if newTotal > cap(req.raw) {
		raw := make([]byte, newTotal)
		copy(raw, req.raw[:tokenStart])
		copy(raw[tokenStart:], encoded)
		copy(raw[tokenStart+len(encoded):], req.raw[tokenEnd:])
		req.raw = raw
	} else {
		oldTotal := len(req.raw)
		req.raw = req.raw[:newTotal]
		copy(req.raw[tokenStart+len(encoded):], req.raw[tokenEnd:oldTotal])
		copy(req.raw[tokenStart:], encoded)
	}
	if req.rawBox != nil {
		*req.rawBox = req.raw
	}
	return nil
}
