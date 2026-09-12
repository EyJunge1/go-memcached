package memcached

import "sync"

// Power-of-two sized pools for hot-path scratch buffers (optional Release).
var _sizedPools = [...]sync.Pool{
	{New: func() any { b := make([]byte, 0, 64); return &b }},
	{New: func() any { b := make([]byte, 0, 128); return &b }},
	{New: func() any { b := make([]byte, 0, 256); return &b }},
	{New: func() any { b := make([]byte, 0, 512); return &b }},
	{New: func() any { b := make([]byte, 0, 1024); return &b }},
	{New: func() any { b := make([]byte, 0, 2048); return &b }},
	{New: func() any { b := make([]byte, 0, 4096); return &b }},
	{New: func() any { b := make([]byte, 0, 8192); return &b }},
	{New: func() any { b := make([]byte, 0, 16384); return &b }},
	{New: func() any { b := make([]byte, 0, 32768); return &b }},
	{New: func() any { b := make([]byte, 0, 65536); return &b }},
	{New: func() any { b := make([]byte, 0, 1<<20); return &b }},
}

// getSizedBytes returns a []byte with len 0 and cap >= need from a sized pool.
// When need is larger than 1 MiB, a fresh buffer is allocated.
// The returned slice is always non-nil.
// Callers may releaseBytes when finished; Release is optional (not required).
func getSizedBytes(need int) []byte {
	if need < 0 {
		need = 0
	}
	if i := sizedPoolIndex(need); i >= 0 {
		box, ok := _sizedPools[i].Get().(*[]byte)
		if !ok || box == nil {
			b := make([]byte, 0, 64<<i)
			if i == len(_sizedPools)-1 {
				b = make([]byte, 0, 1<<20)
			}
			box = &b
		}
		out := (*box)[:0]
		if out == nil {
			want := 64 << i
			if i == len(_sizedPools)-1 {
				want = 1 << 20
			}
			return make([]byte, 0, want)
		}
		return out
	}
	return make([]byte, 0, need)
}

// releaseBytes returns buf to a sized pool when cap matches a pool class.
// It is safe to omit; buffers not from getSizedBytes are ignored.
func releaseBytes(buf []byte) {
	if buf == nil {
		return
	}
	c := cap(buf)
	if i := sizedPoolIndex(c); i >= 0 {
		// Only recycle exact class capacities produced by the pool New funcs.
		want := 64 << i
		if i == len(_sizedPools)-1 {
			want = 1 << 20
		}
		if c != want {
			return
		}
		box := buf[:0]
		_sizedPools[i].Put(&box)
	}
}

func sizedPoolIndex(need int) int {
	poolCap := 64
	for i := range _sizedPools {
		if need <= poolCap {
			return i
		}
		poolCap <<= 1
	}
	return -1
}
