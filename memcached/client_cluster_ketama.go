package memcached

import (
	"crypto/md5" //nolint:gosec // G501: Ketama requires MD5
	"fmt"
	"math"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
)

const (
	ketamaDefaultPort              = 11211
	ketamaPointsPerServerW         = 160
	ketamaWeightedEpsilon  float32 = 0.0000000001
)

// ketamaPickBuilder builds the default weighted Ketama picker.
type ketamaPickBuilder struct{}

// defaultPickBuilder returns the weighted Ketama/MD5 picker used by New.
func defaultPickBuilder() builder {
	return ketamaPickBuilder{}
}

// Build returns a ketamaPicker; the continuum is built lazily on the first Pick.
func (b ketamaPickBuilder) Build(_ []*addr) picker {
	return &ketamaPicker{points: ketamaPointsPerServerW}
}

// ketamaPoint is one hash ring entry.
type ketamaPoint struct {
	value uint32
	index int
}

// ketamaHot is a cached continuum for one membership snapshot.
type ketamaHot struct {
	cacheKey  string
	continuum []ketamaPoint
}

// ketamaPicker is the weighted Ketama/MD5 continuum used by the default cluster.
type ketamaPicker struct {
	points int
	hot    atomic.Pointer[ketamaHot]
}

// Pick selects the cluster member that owns key on the weighted Ketama continuum.
func (p *ketamaPicker) Pick(addrs []*addr, key []byte) (*addr, error) {
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

	h := ketamaDigest(key)
	idx := cont[ketamaLookup(cont, h)].index
	if idx < 0 || idx >= n {
		return nil, fmt.Errorf("no available address: %w", ErrInvalidAddress)
	}
	return addrs[idx], nil
}

// continuumFor returns the cached Ketama ring, rebuilding it when membership or weights change.
func (p *ketamaPicker) continuumFor(addrs []*addr) ([]ketamaPoint, error) {
	key := ketamaCacheKey(addrs)
	if h := p.hot.Load(); h != nil && h.cacheKey == key && h.continuum != nil {
		return h.continuum, nil
	}
	cont, err := buildKetamaContinuum(addrs, p.points)
	if err != nil {
		return nil, err
	}
	p.hot.Store(&ketamaHot{cacheKey: key, continuum: cont})
	return cont, nil
}

// ketamaCacheKey is the identity of a membership snapshot used to reuse a continuum.
func ketamaCacheKey(addrs []*addr) string {
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

// buildKetamaContinuum lays out the weighted Ketama/MD5 ring matching libmemcached 1.1.4.
func buildKetamaContinuum(addrs []*addr, pointsPerServer int) ([]ketamaPoint, error) {
	live := len(addrs)
	if live == 0 {
		return nil, fmt.Errorf("no available address: %w", ErrInvalidAddress)
	}

	var totalWeight uint64
	for _, a := range addrs {
		totalWeight += uint64(a.ketamaWeight())
	}
	if totalWeight == 0 {
		return nil, fmt.Errorf("no available address: %w", ErrInvalidAddress)
	}

	cont := make([]ketamaPoint, 0, live*pointsPerServer)
	for hostIndex, addr := range addrs {
		// libmemcached hosts.cc 1.1.4: floor(pct * 160 / 4 * live + epsilon) * 4
		pct := float32(addr.ketamaWeight()) / float32(totalWeight)
		inner := float32(pct*float32(ketamaPointsPerServerW)/4*float32(live) + ketamaWeightedEpsilon)
		pointerPerServer := uint32(math.Floor(float64(inner))) * 4
		pointerPerHash := uint32(4)

		hashes := pointerPerServer / pointerPerHash
		for pointerIndex := uint32(1); pointerIndex <= hashes; pointerIndex++ {
			hostKey := ketamaHostKey(addr, pointerIndex-1)
			for x := range pointerPerHash {
				cont = append(cont, ketamaPoint{
					value: ketamaServerHash(hostKey, x),
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

// ketamaHostKey formats the host-pointer string hashed into continuum points.
func ketamaHostKey(node *addr, pointer uint32) string {
	host, port := ketamaHostPort(node)
	if port == ketamaDefaultPort {
		return host + "-" + strconv.FormatUint(uint64(pointer), 10)
	}
	return host + ":" + strconv.FormatUint(uint64(port), 10) + "-" + strconv.FormatUint(uint64(pointer), 10)
}

// ketamaHostPort splits node.Address into host and port, treating Unix paths as host-only.
func ketamaHostPort(node *addr) (host string, port uint32) {
	if node == nil {
		return "", 0
	}
	if strings.HasPrefix(node.Network, "unix") {
		return node.Address, 0
	}
	h, p, err := net.SplitHostPort(node.Address)
	if err != nil {
		return node.Address, 0
	}
	n, err := strconv.ParseUint(p, 10, 32)
	if err != nil {
		return h, 0
	}
	return h, uint32(n)
}

// ketamaLookup returns the first continuum index whose point is at least h, wrapping to 0.
func ketamaLookup(cont []ketamaPoint, h uint32) int {
	i := sort.Search(len(cont), func(i int) bool {
		return cont[i].value >= h
	})
	if i == len(cont) {
		return 0
	}
	return i
}

// ketamaServerHash returns one little-endian uint32 from the MD5 of key at alignment.
func ketamaServerHash(key string, alignment uint32) uint32 {
	sum := md5.Sum([]byte(key)) //nolint:gosec // G401: Ketama requires MD5
	off := alignment * 4
	return uint32(sum[off]) |
		uint32(sum[off+1])<<8 |
		uint32(sum[off+2])<<16 |
		uint32(sum[off+3])<<24
}

// ketamaDigest hashes a request key to the first little-endian uint32 of its MD5.
func ketamaDigest(key []byte) uint32 {
	sum := md5.Sum(key) //nolint:gosec // G401: Ketama requires MD5
	return uint32(sum[0]) | uint32(sum[1])<<8 | uint32(sum[2])<<16 | uint32(sum[3])<<24
}
