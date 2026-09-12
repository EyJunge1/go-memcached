package memcached

import (
	"bytes"
	"fmt"
	"strconv"
	"sync"
)

const _metaPipelineIndexThreshold = 16

type metaPipelineIndex struct {
	dense []int32
	num   map[uint64]int
	tok   map[string]int
}

const (
	_maxPooledMetaPipelineIndex = 256
	_maxDenseMetaOpaque         = 4096
)

var _metaPipelineIndexPool = sync.Pool{
	New: func() any { return &metaPipelineIndex{} },
}

func takeMetaPipelineIndex() *metaPipelineIndex {
	index, ok := _metaPipelineIndexPool.Get().(*metaPipelineIndex)
	if !ok || index == nil {
		index = &metaPipelineIndex{}
	}
	index.dense = index.dense[:0]
	clear(index.num)
	clear(index.tok)
	return index
}

func putMetaPipelineIndex(index *metaPipelineIndex) {
	if index == nil {
		return
	}
	if cap(index.dense) > _maxDenseMetaOpaque ||
		len(index.num)+len(index.tok) > _maxPooledMetaPipelineIndex {
		return
	}
	clear(index.dense)
	clear(index.num)
	clear(index.tok)
	index.dense = index.dense[:0]
	_metaPipelineIndexPool.Put(index)
}

func preparedMetaOpaque(p *preparedMetaOp) (num uint64, numeric bool, tok string, ok bool) {
	if !p.hasTok {
		return 0, false, "", false
	}
	if p.tok == "" {
		return p.tokNum, true, "", true
	}
	n, err := strconv.ParseUint(p.tok, 10, 64)
	if err == nil && opaqueStringIsCanonical(p.tok, n) {
		return n, true, "", true
	}
	return 0, false, p.tok, true
}

func samePreparedMetaOpaque(a, b *preparedMetaOp) bool {
	an, anum, atok, aok := preparedMetaOpaque(a)
	bn, bnum, btok, bok := preparedMetaOpaque(b)
	if !aok || !bok || anum != bnum {
		return false
	}
	if anum {
		return an == bn
	}
	return atok == btok
}

func buildMetaPipelineIndex(group []preparedMetaOp) (*metaPipelineIndex, error) {
	if len(group) < _metaPipelineIndexThreshold {
		for i := range group {
			left := &group[i]
			rest := group[i+1:]
			for j := range rest {
				if samePreparedMetaOpaque(left, &rest[j]) {
					return nil, fmt.Errorf("duplicate opaque: %w", ErrInvalidArgument)
				}
			}
		}
		return nil, nil
	}
	index := takeMetaPipelineIndex()
	maxNum := uint64(0)
	numericCount := 0
	tokenCount := 0
	for i := range group {
		num, numeric, _, ok := preparedMetaOpaque(&group[i])
		if !ok {
			continue
		}
		if numeric {
			numericCount++
			if num > maxNum {
				maxNum = num
			}
			continue
		}
		tokenCount++
	}
	useDense := numericCount > 0 && maxNum < _maxDenseMetaOpaque &&
		maxNum+1 <= uint64(len(group))*8
	if useDense {
		need := int(maxNum) + 1
		if cap(index.dense) < need {
			index.dense = make([]int32, need)
		}
		index.dense = index.dense[:need]
		for i := range index.dense {
			index.dense[i] = -1
		}
	}
	for i := range group {
		num, numeric, tok, ok := preparedMetaOpaque(&group[i])
		if !ok {
			continue
		}
		if numeric {
			if useDense {
				if index.dense[num] >= 0 {
					putMetaPipelineIndex(index)
					return nil, fmt.Errorf("duplicate opaque: %w", ErrInvalidArgument)
				}
				index.dense[num] = int32(i)
				continue
			}
			if index.num == nil {
				index.num = make(map[uint64]int, numericCount)
			}
			if _, exists := index.num[num]; exists {
				putMetaPipelineIndex(index)
				return nil, fmt.Errorf("duplicate opaque: %w", ErrInvalidArgument)
			}
			index.num[num] = i
			continue
		}
		if index.tok == nil {
			index.tok = make(map[string]int, tokenCount)
		}
		if _, exists := index.tok[tok]; exists {
			putMetaPipelineIndex(index)
			return nil, fmt.Errorf("duplicate opaque %q: %w", tok, ErrInvalidArgument)
		}
		index.tok[tok] = i
	}
	return index, nil
}

func matchMetaPipelineOpaque(
	header []byte, group []preparedMetaOp, index *metaPipelineIndex,
) (int, bool) {
	num, hasNum, tok := scanMetaOpaque(header)
	if index != nil {
		if hasNum {
			if num < uint64(len(index.dense)) {
				i := index.dense[num]
				return int(i), i >= 0
			}
			i, ok := index.num[num]
			return i, ok
		}
		if len(tok) > 0 {
			i, ok := index.tok[string(tok)]
			return i, ok
		}
		return 0, false
	}
	if hasNum {
		for i := range group {
			n, numeric, _, ok := preparedMetaOpaque(&group[i])
			if ok && numeric && n == num {
				return i, true
			}
		}
	}
	if len(tok) > 0 {
		for i := range group {
			_, numeric, opaque, ok := preparedMetaOpaque(&group[i])
			if ok && !numeric && eqStringBytes(opaque, tok) {
				return i, true
			}
		}
	}
	return 0, false
}

func scanMetaOpaque(header []byte) (num uint64, hasNum bool, tok []byte) {
	line := trimCRLF(header)
	before, after, ok := bytes.Cut(line, []byte{' '})
	if !ok {
		return 0, false, nil
	}
	cd := before
	rest := after
	if bytes.Equal(cd, _VABytes) {
		sp := bytes.IndexByte(rest, ' ')
		if sp < 0 {
			return 0, false, nil
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
				return n, true, nil
			}
			return 0, false, val
		}
		start = i + 1
	}
	return 0, false, nil
}
