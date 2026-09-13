package memcached

import (
	"crypto/md5"
	"fmt"
	"hash/crc32"
	"math"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// libmemcached-awesome 1.1.4 continuum constants (hosts.cc / constants).
const (
	ketamaDefaultPort              = 11211
	ketamaPointsPerServer          = 100
	ketamaPointsPerServerW         = 160
	ketamaWeightedEpsilon  float32 = 0.0000000001
)

// KetamaHash selects the hashkit algorithm used for unweighted continuum
// points and for hashing item keys. Weighted continuum points always use MD5
// (ketama_server_hash), matching libmemcached 1.1.4.
type KetamaHash int

const (
	// KetamaHashMD5 is the libmemcached KETAMA default (interop with php-memcached).
	KetamaHashMD5 KetamaHash = iota
	// KetamaHashCRC is hashkit CRC (not IEEE CRC-32).
	KetamaHashCRC
	// KetamaHashFNV1A is hashkit FNV-1a 32-bit.
	KetamaHashFNV1A
)

// KetamaOptions configures NewKetamaPickBuilder.
//
// The default picker of this package is CRC32-IEEE modulo N — not Ketama.
// Use this builder only when sharing a cluster with libmemcached / php-memcached.
// php-memcached OPT_LIBKETAMA_COMPATIBLE is Weighted: true and Hash MD5.
type KetamaOptions struct {
	Hash            KetamaHash
	Weighted        bool
	PointsPerServer int // 0 → 100 unweighted / 160 weighted
}

type ketamaPickBuilder struct {
	opts KetamaOptions
}

// NewKetamaPickBuilder returns a picker compatible with libmemcached-awesome
// 1.1.4 MEMCACHED_DISTRIBUTION_CONSISTENT_KETAMA (and KETAMA_WEIGHTED).
// It is not compatible with Ketama-SPY (Spymemcached).
func NewKetamaPickBuilder(o KetamaOptions) Builder {
	return ketamaPickBuilder{opts: o}
}

func (b ketamaPickBuilder) Build(_ []*Addr) Picker {
	points := b.opts.PointsPerServer
	if points <= 0 {
		if b.opts.Weighted {
			points = ketamaPointsPerServerW
		} else {
			points = ketamaPointsPerServer
		}
	}
	return &ketamaPicker{
		hash:     b.opts.Hash,
		weighted: b.opts.Weighted,
		points:   points,
	}
}

type ketamaPoint struct {
	value uint32
	index int
}

type ketamaPicker struct {
	hash     KetamaHash
	weighted bool
	points   int

	mu        sync.Mutex
	cacheKey  string
	continuum []ketamaPoint
}

func (p *ketamaPicker) Pick(addrs []*Addr, _, key []byte) (*Addr, error) {
	n := len(addrs)
	if n == 0 {
		return nil, fmt.Errorf("no available address: %w", ErrInvalidAddress)
	}
	if n == 1 {
		return addrs[0], nil
	}

	cont, err := p.continuumFor(addrs)
	if err != nil {
		return nil, err
	}
	if len(cont) == 0 {
		return nil, fmt.Errorf("no available address: %w", ErrInvalidAddress)
	}

	h := ketamaDigest(p.hash, key)
	idx := cont[ketamaLookup(cont, h)].index
	if idx < 0 || idx >= n {
		return nil, fmt.Errorf("no available address: %w", ErrInvalidAddress)
	}
	return addrs[idx], nil
}

func (p *ketamaPicker) continuumFor(addrs []*Addr) ([]ketamaPoint, error) {
	key := ketamaCacheKey(addrs)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cacheKey == key && p.continuum != nil {
		return p.continuum, nil
	}
	cont, err := buildKetamaContinuum(addrs, p.hash, p.weighted, p.points)
	if err != nil {
		return nil, err
	}
	p.cacheKey = key
	p.continuum = cont
	return cont, nil
}

func ketamaCacheKey(addrs []*Addr) string {
	var b strings.Builder
	for i, a := range addrs {
		if i > 0 {
			b.WriteByte(0)
		}
		if a == nil {
			continue
		}
		b.WriteString(a.id())
		b.WriteByte(0)
		b.WriteString(strconv.FormatUint(uint64(a.ketamaWeight()), 10))
	}
	return b.String()
}

func buildKetamaContinuum(addrs []*Addr, hash KetamaHash, weighted bool, pointsPerServer int) ([]ketamaPoint, error) {
	live := len(addrs)
	if live == 0 {
		return nil, fmt.Errorf("no available address: %w", ErrInvalidAddress)
	}

	var totalWeight uint64
	if weighted {
		for _, a := range addrs {
			totalWeight += uint64(a.ketamaWeight())
		}
		if totalWeight == 0 {
			return nil, fmt.Errorf("no available address: %w", ErrInvalidAddress)
		}
	}

	cont := make([]ketamaPoint, 0, live*pointsPerServer)
	for hostIndex, addr := range addrs {
		pointerPerServer := uint32(pointsPerServer)
		pointerPerHash := uint32(1)
		if weighted {
			// hosts.cc 1.1.4: floor((float)(pct * 160 / 4 * live + 0.0000000001F)) * 4
			pct := float32(addr.ketamaWeight()) / float32(totalWeight)
			inner := float32(pct*float32(ketamaPointsPerServerW)/4*float32(live) + ketamaWeightedEpsilon)
			pointerPerServer = uint32(math.Floor(float64(inner))) * 4
			pointerPerHash = 4
		}

		hashes := pointerPerServer / pointerPerHash
		for pointerIndex := uint32(1); pointerIndex <= hashes; pointerIndex++ {
			hostKey := ketamaHostKey(addr, pointerIndex-1)
			if weighted {
				for x := uint32(0); x < pointerPerHash; x++ {
					cont = append(cont, ketamaPoint{
						value: ketamaServerHash(hostKey, x),
						index: hostIndex,
					})
				}
			} else {
				cont = append(cont, ketamaPoint{
					value: ketamaDigest(hash, []byte(hostKey)),
					index: hostIndex,
				})
			}
		}
	}

	sort.SliceStable(cont, func(i, j int) bool {
		if cont[i].value == cont[j].value {
			return cont[i].index < cont[j].index
		}
		return cont[i].value < cont[j].value
	})
	return cont, nil
}

func ketamaHostKey(addr *Addr, pointer uint32) string {
	host, port := ketamaHostPort(addr)
	if port == ketamaDefaultPort {
		return host + "-" + strconv.FormatUint(uint64(pointer), 10)
	}
	return host + ":" + strconv.FormatUint(uint64(port), 10) + "-" + strconv.FormatUint(uint64(pointer), 10)
}

func ketamaHostPort(addr *Addr) (host string, port uint32) {
	if addr == nil {
		return "", 0
	}
	if strings.HasPrefix(addr.Network, "unix") {
		return addr.Address, 0
	}
	h, p, err := net.SplitHostPort(addr.Address)
	if err != nil {
		return addr.Address, 0
	}
	n, err := strconv.ParseUint(p, 10, 32)
	if err != nil {
		return h, 0
	}
	return h, uint32(n)
}

// ketamaLookup is libmemcached dispatch_host: first continuum point with
// value >= h, wrapping to index 0 when the hash sits past the last point.
func ketamaLookup(cont []ketamaPoint, h uint32) int {
	i := sort.Search(len(cont), func(i int) bool {
		return cont[i].value >= h
	})
	if i == len(cont) {
		return 0
	}
	return i
}

func ketamaServerHash(key string, alignment uint32) uint32 {
	sum := md5.Sum([]byte(key))
	off := alignment * 4
	return uint32(sum[off]) |
		uint32(sum[off+1])<<8 |
		uint32(sum[off+2])<<16 |
		uint32(sum[off+3])<<24
}

func ketamaDigest(h KetamaHash, key []byte) uint32 {
	switch h {
	case KetamaHashCRC:
		return hashkitCRC32(key)
	case KetamaHashFNV1A:
		return hashkitFNV1A32(key)
	default:
		sum := md5.Sum(key)
		return uint32(sum[0]) | uint32(sum[1])<<8 | uint32(sum[2])<<16 | uint32(sum[3])<<24
	}
}

// hashkitCRC32 matches libhashkit_crc32: IEEE CRC-32, then ((~crc)>>16)&0x7fff.
func hashkitCRC32(key []byte) uint32 {
	return (crc32.ChecksumIEEE(key) >> 16) & 0x7fff
}

func hashkitFNV1A32(key []byte) uint32 {
	hash := uint32(2166136261)
	for _, b := range key {
		// libhashkit: uint32_t val = (uint32_t) key[x] with signed char (Linux/macOS).
		hash ^= uint32(int8(b))
		hash *= 16777619
	}
	return hash
}
