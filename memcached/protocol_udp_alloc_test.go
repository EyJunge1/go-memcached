package memcached

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestUDPRequestAllowedAllocs(t *testing.T) {
	req := buildRequest(_GetBytes, []byte("k"), []byte("get k\r\n"))
	defer req.release()
	allocs := testing.AllocsPerRun(200, func() {
		if err := udpRequestAllowed(req); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("udpRequestAllowed allocs = %v, want 0", allocs)
	}
}

func TestUDPMetaRawAllowedAllocs(t *testing.T) {
	raw := []byte("mg k v\r\n")
	allocs := testing.AllocsPerRun(200, func() {
		if err := udpMetaRawAllowed(raw); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("udpMetaRawAllowed allocs = %v, want 0", allocs)
	}
}

func TestUDPMetaRawAllowedRejectsSideEffect(t *testing.T) {
	for _, flag := range []string{"N30", "R30", "T30", "Eabc"} {
		t.Run(flag, func(t *testing.T) {
			isErrorIs(t, udpMetaRawAllowed([]byte("mg k "+flag+"\r\n")), ErrNotSupported)
		})
	}
	isErrorIs(t, udpMetaRawAllowed([]byte("ms k\r\n")), ErrNotSupported)
	noError(t, udpMetaRawAllowed([]byte("mg k v\r\n")))
	noError(t, udpMetaRawAllowed([]byte("mn\r\n")))
	noError(t, udpMetaRawAllowed([]byte("me k\r\n")))
}

func TestAssembleUDPThreeFragmentsInOrderAllocs(t *testing.T) {
	makeDgram := func(seq, total uint16, payload []byte) []byte {
		h := make([]byte, 8+len(payload))
		binary.BigEndian.PutUint16(h[0:2], 3)
		binary.BigEndian.PutUint16(h[2:4], seq)
		binary.BigEndian.PutUint16(h[4:6], total)
		copy(h[8:], payload)
		return h
	}
	datagrams := [][]byte{
		makeDgram(0, 3, bytes.Repeat([]byte("A"), 1392)),
		makeDgram(1, 3, bytes.Repeat([]byte("B"), 1392)),
		makeDgram(2, 3, bytes.Repeat([]byte("C"), 1400)),
	}
	cn := &recordUDPConn{mockConn: newMockConn(), datagrams: datagrams}
	allocs := testing.AllocsPerRun(200, func() {
		cn.di = 0
		box, err := assembleUDP(cn, 3, udpFragmentCap(0))
		if err != nil {
			t.Fatal(err)
		}
		putUDPBuf(box)
	})
	if allocs != 0 {
		t.Fatalf("assembleUDP tot=3 allocs = %v, want 0", allocs)
	}
}

func TestAssembleUDPSingleFragmentAllocs(t *testing.T) {
	payload := []byte("VALUE k 0 1\r\nx\r\nEND\r\n")
	d := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint16(d[0:2], 3)
	binary.BigEndian.PutUint16(d[4:6], 1)
	copy(d[8:], payload)
	cn := &recordUDPConn{mockConn: newMockConn(), datagrams: [][]byte{d}}
	allocs := testing.AllocsPerRun(200, func() {
		cn.di = 0
		box, err := assembleUDP(cn, 3, udpFragmentCap(0))
		if err != nil {
			t.Fatal(err)
		}
		putUDPBuf(box)
	})
	if allocs != 0 {
		t.Fatalf("assembleUDP tot=1 allocs = %v, want 0", allocs)
	}
}

func TestSendUDPAllocs(t *testing.T) {
	cn := newMockConn()
	req := buildRequest([]byte("version"), nil, []byte("version\r\n"))
	req.udpEnabled = true
	req.udpReqID = 7
	defer req.release()
	allocs := testing.AllocsPerRun(200, func() {
		if err := req.sendUDP(cn); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("sendUDP allocs = %v, want 0", allocs)
	}
}
