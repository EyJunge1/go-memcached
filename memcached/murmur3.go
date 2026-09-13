package memcached

const (
	murmur3c1 = uint64(0x87c37b91114253d5)
	murmur3c2 = uint64(0x4cf5ad432745937f)
)

// murmur3 implements the Murmur3 digest 64 algorithm.
type murmur3 struct {
	seed uint64
}

func newMurmur3(seed uint64) *murmur3 {
	return &murmur3{seed: seed}
}

func (h *murmur3) hash(key []byte) uint64 {
	return h.hash2(key, nil)
}

func copyJoin(dst, a, b []byte, off int) {
	n := len(dst)
	if off < len(a) {
		take := len(a) - off
		if take > n {
			take = n
		}
		copy(dst, a[off:off+take])
		if take == n {
			return
		}
		copy(dst[take:], b[:n-take])
		return
	}
	start := off - len(a)
	copy(dst, b[start:start+n])
}

func (h *murmur3) hash2(a, b []byte) uint64 {
	length := len(a) + len(b)
	sum := h.seed

	nblocks := length / 8
	var block [8]byte
	for i := range nblocks {
		copyJoin(block[:], a, b, i*8)
		k := uint64(block[0]) | uint64(block[1])<<8 |
			uint64(block[2])<<16 | uint64(block[3])<<24 |
			uint64(block[4])<<32 | uint64(block[5])<<40 |
			uint64(block[6])<<48 | uint64(block[7])<<56

		k *= murmur3c1
		k = (k << 31) | (k >> 33)
		k *= murmur3c2

		sum ^= k
		sum = (sum << 27) | (sum >> 37)
		sum = sum*5 + 0x52dce729
	}

	rem := length & 7
	if rem > 0 {
		var tail [7]byte
		copyJoin(tail[:rem], a, b, nblocks*8)
		k2 := uint64(0)
		switch rem {
		case 7:
			k2 ^= uint64(tail[6]) << 48
			fallthrough
		case 6:
			k2 ^= uint64(tail[5]) << 40
			fallthrough
		case 5:
			k2 ^= uint64(tail[4]) << 32
			fallthrough
		case 4:
			k2 ^= uint64(tail[3]) << 24
			fallthrough
		case 3:
			k2 ^= uint64(tail[2]) << 16
			fallthrough
		case 2:
			k2 ^= uint64(tail[1]) << 8
			fallthrough
		case 1:
			k2 ^= uint64(tail[0])
			k2 *= murmur3c1
			k2 = (k2 << 31) | (k2 >> 33)
			k2 *= murmur3c2
			sum ^= k2
		}
	}

	sum ^= uint64(length)
	sum ^= sum >> 33
	sum *= 0xff51afd7ed558ccd
	sum ^= sum >> 33
	sum *= 0xc4ceb9fe1a85ec53
	sum ^= sum >> 33

	return sum
}
