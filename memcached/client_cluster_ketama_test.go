package memcached

import (
	"crypto/md5"
	"strconv"
	"testing"
)

func Test_splitAddrWeight(t *testing.T) {
	raw, w, err := splitAddrWeight("localhost:11211?weight=2")
	noError(t, err)
	equal(t, "localhost:11211", raw)
	equal(t, uint32(2), w)

	raw, w, err = splitAddrWeight("localhost:11211/?weight=2")
	noError(t, err)
	equal(t, "localhost:11211", raw)
	equal(t, uint32(2), w)

	raw, w, err = splitAddrWeight("[::1]:11211?weight=3")
	noError(t, err)
	equal(t, "[::1]:11211", raw)
	equal(t, uint32(3), w)

	raw, w, err = splitAddrWeight("[::1]:11211/?weight=3")
	noError(t, err)
	equal(t, "[::1]:11211", raw)
	equal(t, uint32(3), w)

	raw, w, err = splitAddrWeight("localhost:11211")
	noError(t, err)
	equal(t, "localhost:11211", raw)
	equal(t, uint32(0), w)

	raw, w, err = splitAddrWeight("localhost:11211?weight=0")
	noError(t, err)
	equal(t, "localhost:11211", raw)
	equal(t, uint32(0), w)

	_, _, err = splitAddrWeight("localhost:11211?foo=1")
	isErrorIs(t, err, ErrInvalidAddress)

	_, _, err = splitAddrWeight("localhost:11211/?foo=1")
	isErrorIs(t, err, ErrInvalidAddress)

	_, _, err = splitAddrWeight("localhost:11211?weight=-1")
	isErrorIs(t, err, ErrInvalidAddress)
}

func Test_defaultResolver_Resolve_weight(t *testing.T) {
	addrs, err := newDefaultResolver().Resolve("localhost:11211?weight=2,localhost:11212")
	noError(t, err)
	hasLen(t, addrs, 2)
	equal(t, uint32(2), addrs[0].Weight)
	equal(t, uint32(0), addrs[1].Weight)
	equal(t, "localhost:11211", addrs[0].Address)
	equal(t, "localhost:11212", addrs[1].Address)

	addrs, err = newDefaultResolver().Resolve("[::1]:11211?weight=4")
	noError(t, err)
	hasLen(t, addrs, 1)
	equal(t, uint32(4), addrs[0].Weight)
	equal(t, "[::1]:11211", addrs[0].Address)

	addrs, err = newDefaultResolver().Resolve("10.0.0.1:11211/?weight=5")
	noError(t, err)
	hasLen(t, addrs, 1)
	equal(t, uint32(5), addrs[0].Weight)
	equal(t, "10.0.0.1:11211", addrs[0].Address)

	addrs, err = newDefaultResolver().Resolve("unix:///tmp/memcached.sock?weight=2")
	noError(t, err)
	hasLen(t, addrs, 1)
	equal(t, "unix", addrs[0].Network)
	equal(t, "/tmp/memcached.sock", addrs[0].Address)
	equal(t, uint32(2), addrs[0].Weight)
}

func Test_ketamaHostKey_defaultPortOmitsPort(t *testing.T) {
	equal(t, "10.0.0.1-0", ketamaHostKey(newAddrWeighted("tcp", "10.0.0.1:11211", 0), 0))
	equal(t, "10.0.0.1:11212-3", ketamaHostKey(newAddrWeighted("tcp", "10.0.0.1:11212", 0), 3))
	equal(t, "/tmp/memcached.sock:0-0", ketamaHostKey(newAddrWeighted("unix", "/tmp/memcached.sock", 0), 0))
	equal(t, "::1-0", ketamaHostKey(newAddrWeighted("tcp", "[::1]:11211", 0), 0))
	equal(t, "::1:11212-1", ketamaHostKey(newAddrWeighted("tcp", "[::1]:11212", 0), 1))
}

func Test_ketamaDigest_md5LittleEndian(t *testing.T) {
	sum := md5.Sum([]byte("10.0.0.1-0"))
	want := uint32(sum[0]) | uint32(sum[1])<<8 | uint32(sum[2])<<16 | uint32(sum[3])<<24
	equal(t, want, ketamaDigest([]byte("10.0.0.1-0")))
	equal(t, want, ketamaServerHash("10.0.0.1-0", 0))
}

func Test_ketamaPick_goldenWeightedTwoNodes(t *testing.T) {
	addrs := []*addr{
		newAddrWeighted("tcp", "10.0.0.1:11211", 1),
		newAddrWeighted("tcp", "10.0.0.2:11211", 2),
	}
	picker := defaultPickBuilder().Build(addrs)

	fixtures := map[string]string{
		"hello":       "10.0.0.2:11211",
		"user:1":      "10.0.0.2:11211",
		"session:abc": "10.0.0.1:11211",
		"a":           "10.0.0.2:11211",
		"zzzzzzzz":    "10.0.0.1:11211",
		"":            "10.0.0.2:11211",
		"foo":         "10.0.0.2:11211",
		"bar":         "10.0.0.2:11211",
		"ketama":      "10.0.0.2:11211",
		"ns:key":      "10.0.0.1:11211",
	}
	for key, want := range fixtures {
		got, err := picker.Pick(addrs, []byte(key))
		noError(t, err)
		equal(t, want, got.Address)
	}
}

func Test_ketamaLookup_wrapsPastLastPoint(t *testing.T) {
	cont := []ketamaPoint{{value: 10, index: 1}, {value: 20, index: 0}}
	equal(t, 0, ketamaLookup(cont, 0))
	equal(t, 0, ketamaLookup(cont, 10))
	equal(t, 1, ketamaLookup(cont, 11))
	equal(t, 1, ketamaLookup(cont, 20))
	equal(t, 0, ketamaLookup(cont, 21))
}

func Test_ketamaWeighted_morePointsOnHeavierNode(t *testing.T) {
	addrs := []*addr{
		newAddrWeighted("tcp", "10.0.0.1:11211", 1),
		newAddrWeighted("tcp", "10.0.0.2:11211", 2),
	}
	cont, err := buildKetamaContinuum(addrs, ketamaPointsPerServerW)
	noError(t, err)
	var c0, c1 int
	for _, p := range cont {
		if p.index == 0 {
			c0++
		} else {
			c1++
		}
	}
	if c1 <= c0 {
		t.Fatalf("weight=2 node should have more points: light=%d heavy=%d", c0, c1)
	}
	equal(t, 104, c0)
	equal(t, 212, c1)
}

func Test_ketamaEject_onlyDeadNodeKeysMove(t *testing.T) {
	full := []*addr{
		newAddrWeighted("tcp", "10.0.0.1:11211", 0),
		newAddrWeighted("tcp", "10.0.0.2:11211", 0),
		newAddrWeighted("tcp", "10.0.0.3:11211", 0),
	}
	live := []*addr{full[0], full[2]}
	picker := defaultPickBuilder().Build(full)

	moved, stayed := 0, 0
	for i := 0; i < 200; i++ {
		key := []byte("k" + strconv.Itoa(i))
		before, err := picker.Pick(full, key)
		noError(t, err)
		after, err := picker.Pick(live, key)
		noError(t, err)
		if before.Address == "10.0.0.2:11211" {
			moved++
			if after.Address == "10.0.0.2:11211" {
				t.Fatal("ejected node still selected")
			}
			continue
		}
		stayed++
		equal(t, before.Address, after.Address)
	}
	if moved == 0 || stayed == 0 {
		t.Fatalf("expected both moved and stayed keys, moved=%d stayed=%d", moved, stayed)
	}
}

func Test_ketamaWeight_zeroMeansOne(t *testing.T) {
	equal(t, uint32(1), (*addr)(nil).ketamaWeight())
	equal(t, uint32(1), newAddrWeighted("tcp", "10.0.0.1:11211", 0).ketamaWeight())
	equal(t, uint32(3), newAddrWeighted("tcp", "10.0.0.1:11211", 3).ketamaWeight())
}
