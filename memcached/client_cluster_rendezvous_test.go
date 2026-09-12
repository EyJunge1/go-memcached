package memcached

import (
	"testing"
)

func TestRendezvousPickStableAndDeterministic(t *testing.T) {
	addrs := []*addr{
		newAddr("tcp", "10.0.0.1:11211"),
		newAddr("tcp", "10.0.0.2:11211"),
		newAddr("tcp", "10.0.0.3:11211"),
	}
	p := rendezvousPickBuilder{}.Build(addrs)

	key := []byte("user:42")
	idx, node, err := pickAddrIndex(p, addrs, key)
	if err != nil {
		t.Fatal(err)
	}
	if node != addrs[idx] {
		t.Fatalf("index/node mismatch")
	}
	for i := 0; i < 20; i++ {
		idx2, node2, err := pickAddrIndex(p, addrs, key)
		if err != nil {
			t.Fatal(err)
		}
		if idx2 != idx || node2 != node {
			t.Fatalf("unstable pick: first=%d/%v later=%d/%v", idx, node, idx2, node2)
		}
	}
}

func TestRendezvousRemapOnMembershipChange(t *testing.T) {
	a := []*addr{
		newAddr("tcp", "10.0.0.1:11211"),
		newAddr("tcp", "10.0.0.2:11211"),
		newAddr("tcp", "10.0.0.3:11211"),
	}
	p := rendezvousPickBuilder{}.Build(a)
	n1, err := p.Pick(a, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	// Drop middle node: remaining owners should stay stable for keys that
	// were not on the removed node (rendezvous property).
	b := []*addr{a[0], a[2]}
	n2, err := p.Pick(b, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if n1 == a[1] {
		// key mapped to removed node — remap is expected
		if n2 != a[0] && n2 != a[2] {
			t.Fatalf("unexpected remap target %v", n2)
		}
		return
	}
	if n2 != n1 {
		t.Fatalf("key remapped away from live owner: before=%v after=%v", n1, n2)
	}
}

func TestDefaultPickBuilderIsRendezvous(t *testing.T) {
	_, ok := defaultPickBuilder().(rendezvousPickBuilder)
	if !ok {
		t.Fatalf("default picker builder = %T", defaultPickBuilder())
	}
}

func TestCleanAddrRejectsQuery(t *testing.T) {
	if _, err := cleanAddr("localhost:11211?weight=2"); err == nil {
		t.Fatal("expected error")
	}
	got, err := cleanAddr("localhost:11211")
	noError(t, err)
	equal(t, "localhost:11211", got)
}

func TestRendezvousPickHotPathDoesNotAllocate(t *testing.T) {
	addrs := []*addr{
		newAddr("tcp", "10.0.0.1:11211"),
		newAddr("tcp", "10.0.0.2:11211"),
		newAddr("tcp", "10.0.0.3:11211"),
	}
	p := rendezvousPickBuilder{}.Build(addrs)
	key := []byte("hot-key")
	if _, err := p.Pick(addrs, key); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		if _, err := p.Pick(addrs, key); err != nil {
			t.Fatal(err)
		}
	})
	equal(t, float64(0), allocs)
}

func TestRendezvousPickIndexMatchesPick(t *testing.T) {
	addrs := []*addr{
		newAddr("tcp", "10.0.0.1:11211"),
		newAddr("tcp", "10.0.0.2:11211"),
	}
	picker, ok := (&rendezvousPickBuilder{}).Build(addrs).(*rendezvousPicker)
	isTrue(t, ok)
	index, indexed, err := picker.PickIndex(addrs, []byte("key"))
	noError(t, err)
	picked, err := picker.Pick(addrs, []byte("key"))
	noError(t, err)
	equal(t, addrs[index], indexed)
	equal(t, picked, indexed)
}
