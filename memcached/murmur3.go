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
	length := len(key)
	sum := h.seed

	nblocks := length / 8
	for i := range nblocks {
		k := uint64(key[i*8]) | uint64(key[i*8+1])<<8 |
			uint64(key[i*8+2])<<16 | uint64(key[i*8+3])<<24 |
			uint64(key[i*8+4])<<32 | uint64(key[i*8+5])<<40 |
			uint64(key[i*8+6])<<48 | uint64(key[i*8+7])<<56

		k *= murmur3c1
		k = (k << 31) | (k >> 33)
		k *= murmur3c2

		sum ^= k
		sum = (sum << 27) | (sum >> 37)
		sum = sum*5 + 0x52dce729
	}

	tail := key[nblocks*8:]
	k2 := uint64(0)
	switch length & 7 {
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

	sum ^= uint64(length)
	sum ^= sum >> 33
	sum *= 0xff51afd7ed558ccd
	sum ^= sum >> 33
	sum *= 0xc4ceb9fe1a85ec53
	sum ^= sum >> 33

	return sum
}
