package memcached

import (
	"bytes"
	"fmt"
	"strconv"
)

func (b *PreparedMetaBatch) refreshRoutes() error {
	for {
		clusterVersion := b.c.clusterVersion.Load()
		healthVersion := b.c.healthVersion.Load()
		addrs, picker := b.c.snapshotCluster()
		if picker == nil || len(addrs) == 0 {
			return fmt.Errorf("no servers configured: %w", ErrInvalidAddress)
		}
		if b.pinned {
			node, err := resolvePinnedAddr(addrs, b.pin)
			if err != nil {
				return err
			}
			if clusterVersion != b.c.clusterVersion.Load() ||
				healthVersion != b.c.healthVersion.Load() {
				continue
			}
			if err := b.buildPinnedGroups(node); err != nil {
				return err
			}
			b.clusterVersion = clusterVersion
			b.healthVersion = healthVersion
			return nil
		}
		addrs = b.c.liveAddrs(addrs)

		if len(b.ops) == 0 {
			releasePreparedMetaGroups(b)
			b.clusterVersion = clusterVersion
			b.healthVersion = healthVersion
			return nil
		}
		if cap(b.routed) < len(b.ops) {
			b.routed = make([]int, len(b.ops))
		}
		b.routed = b.routed[:len(b.ops)]
		if cap(b.counts) < len(addrs) {
			b.counts = make([]int, len(addrs))
			b.offsets = make([]int, len(addrs)+1)
		}
		b.counts = b.counts[:len(addrs)]
		b.offsets = b.offsets[:len(addrs)+1]
		clear(b.counts)
		clear(b.offsets)
		for i := range b.ops {
			nodeIndex, _, err := pickAddrIndex(picker, addrs, b.ops[i].key)
			if err != nil {
				return fmt.Errorf("pick node: %w", err)
			}
			b.routed[i] = nodeIndex
			b.counts[nodeIndex]++
		}
		for i := range addrs {
			b.offsets[i+1] = b.offsets[i] + b.counts[i]
			b.counts[i] = b.offsets[i]
		}
		if cap(b.grouped) < len(b.ops) {
			b.grouped = make([]preparedMetaOp, len(b.ops))
		}
		b.grouped = b.grouped[:len(b.ops)]
		for i, group := range b.routed {
			b.grouped[b.counts[group]] = b.ops[i]
			b.counts[group]++
		}
		if clusterVersion != b.c.clusterVersion.Load() ||
			healthVersion != b.c.healthVersion.Load() {
			continue
		}
		if b.groups == nil {
			b.groups = make([]preparedMetaBatchGroup, 0, len(addrs))
			b.raws = make([][]byte, 0, len(addrs))
		}
		releasePreparedMetaGroups(b)
		for i, node := range addrs {
			start, end := b.offsets[i], b.offsets[i+1]
			if start == end {
				continue
			}
			ops := b.grouped[start:end]
			index, err := buildMetaPipelineIndex(ops)
			if err != nil {
				releasePreparedMetaGroups(b)
				return err
			}
			raw := b.takeRaw()
			if raw == nil {
				raw = encodePreparedMetaBatch(ops)
			} else {
				raw = encodePreparedMetaBatchInto(raw[:0], ops)
			}
			b.groups = append(b.groups, preparedMetaBatchGroup{
				node: node, ops: ops, raw: raw, index: index,
			})
		}
		b.defaultWire = true
		b.clusterVersion = clusterVersion
		b.healthVersion = healthVersion
		return nil
	}
}

func resolvePinnedAddr(addrs []*addr, pin Server) (*addr, error) {
	wantNet := serverNetwork(pin)
	for _, a := range addrs {
		if a != nil && a.Address == pin.Address && serverNetwork(addrToServer(a)) == wantNet {
			return a, nil
		}
	}
	return nil, fmt.Errorf("node target %s: %w", pin.Address, ErrInvalidAddress)
}

func (b *PreparedMetaBatch) buildPinnedGroups(node *addr) error {
	if len(b.ops) == 0 {
		releasePreparedMetaGroups(b)
		return nil
	}
	if b.groups == nil {
		b.groups = make([]preparedMetaBatchGroup, 0, 1)
		b.raws = make([][]byte, 0, 1)
	}
	releasePreparedMetaGroups(b)
	index, err := buildMetaPipelineIndex(b.ops)
	if err != nil {
		return err
	}
	raw := b.takeRaw()
	if raw == nil {
		raw = encodePreparedMetaBatch(b.ops)
	} else {
		raw = encodePreparedMetaBatchInto(raw[:0], b.ops)
	}
	b.groups = append(b.groups, preparedMetaBatchGroup{
		node: node, ops: b.ops, raw: raw, index: index,
	})
	b.defaultWire = true
	return nil
}

func (b *PreparedMetaBatch) rebuildWire(values [][]byte) {
	for i := range b.groups {
		group := &b.groups[i]
		group.raw = encodePreparedMetaBatchValues(
			group.raw[:0], group.ops, b.valueSlot, b.defaultValues, values,
		)
	}
	b.defaultWire = values == nil
}

func (b *PreparedMetaBatch) takeRaw() []byte {
	if len(b.raws) == 0 {
		return nil
	}
	last := len(b.raws) - 1
	raw := b.raws[last]
	b.raws[last] = nil
	b.raws = b.raws[:last]
	return raw
}

func (b *PreparedMetaBatch) release() {
	if b == nil {
		return
	}
	for i := range b.ops {
		putBuilderBox(b.ops[i].lineBox)
		putBuilderBox(b.ops[i].keyBox)
		b.ops[i] = preparedMetaOp{}
	}
	b.ops = nil
	b.dst = nil
	releasePreparedMetaGroups(b)
	b.groups = nil
	b.valueOps = nil
	b.valueSlot = nil
	b.defaultValues = nil
	b.routed = nil
	b.counts = nil
	b.offsets = nil
	b.grouped = nil
	b.raws = nil
	b.pin = Server{}
	b.pinned = false
	b.c = nil
}

func encodePreparedMetaBatch(ops []preparedMetaOp) []byte {
	var barrier MetaNoop
	need := 0
	for i := range ops {
		need += len(ops[i].line) + len(ops[i].body) + len(ops[i].tail)
		if ops[i].noop {
			barrier = ops[i].barrier
		}
	}
	need += metaQuietBarrierLen(barrier)
	return encodePreparedMetaBatchInto(make([]byte, 0, need), ops)
}

func encodePreparedMetaBatchInto(raw []byte, ops []preparedMetaOp) []byte {
	var barrier MetaNoop
	for i := range ops {
		raw = append(raw, ops[i].line...)
		raw = append(raw, ops[i].body...)
		raw = append(raw, ops[i].tail...)
		if ops[i].noop {
			barrier = ops[i].barrier
		}
	}
	return appendMetaQuietBarrier(raw, barrier)
}

func encodePreparedMetaBatchValues(
	raw []byte,
	ops []preparedMetaOp,
	valueSlot []int,
	defaultValues, values [][]byte,
) []byte {
	var barrier MetaNoop
	for i := range ops {
		op := &ops[i]
		body := op.body
		if slot := valueSlot[op.idx]; slot >= 0 {
			body = defaultValues[slot]
			if values != nil {
				body = values[slot]
			}
			raw = appendMetaSetHeaderLength(raw, op.line, len(body))
		} else {
			raw = append(raw, op.line...)
		}
		raw = append(raw, body...)
		raw = append(raw, op.tail...)
		if op.noop {
			barrier = op.barrier
		}
	}
	return appendMetaQuietBarrier(raw, barrier)
}

func appendMetaSetHeaderLength(dst, line []byte, valueLen int) []byte {
	commandEnd := bytes.IndexByte(line, _SpaceByte)
	if commandEnd < 0 {
		return append(dst, line...)
	}
	keyEndRel := bytes.IndexByte(line[commandEnd+1:], _SpaceByte)
	if keyEndRel < 0 {
		return append(dst, line...)
	}
	tokenStart := commandEnd + 1 + keyEndRel + 1
	tokenEndRel := bytes.IndexByte(line[tokenStart:], _SpaceByte)
	tokenEnd := len(line) - len(_CRLFBytes)
	if tokenEndRel >= 0 {
		tokenEnd = tokenStart + tokenEndRel
	}
	dst = append(dst, line[:tokenStart]...)
	dst = strconv.AppendInt(dst, int64(valueLen), 10)
	return append(dst, line[tokenEnd:]...)
}

func releasePreparedMetaGroups(b *PreparedMetaBatch) {
	if b == nil {
		return
	}
	for i := range b.groups {
		putMetaPipelineIndex(b.groups[i].index)
		if b.groups[i].raw != nil {
			b.raws = append(b.raws, b.groups[i].raw[:0])
		}
		b.groups[i] = preparedMetaBatchGroup{}
	}
	b.groups = b.groups[:0]
}
