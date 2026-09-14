package memcached

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"math"
	"net"
	"testing"
	"time"
)

type recordUDPConn struct {
	*mockConn
	writes    [][]byte
	datagrams [][]byte
	di        int
}

func (c *recordUDPConn) Write(p []byte) (int, error) {
	c.writes = append(c.writes, append([]byte(nil), p...))
	return len(p), nil
}

func (c *recordUDPConn) readDatagram() ([]byte, error) {
	if c.di >= len(c.datagrams) {
		return nil, io.EOF
	}
	d := c.datagrams[c.di]
	c.di++
	return d, nil
}

func Test_sendUDPUsesRunningRequestID(t *testing.T) {
	cn := &recordUDPConn{mockConn: newMockConn()}
	req := buildRequest([]byte("version"), nil, []byte("version\r\n"))
	req.udpEnabled = true
	req.udpReqID = 7
	noError(t, req.sendUDP(cn))
	hasLen(t, cn.writes, 1)
	hdr := cn.writes[0]
	greater(t, len(hdr), 8)
	equal(t, uint16(7), binary.BigEndian.Uint16(hdr[0:2]))
	equal(t, uint16(0), binary.BigEndian.Uint16(hdr[2:4]))
	equal(t, uint16(1), binary.BigEndian.Uint16(hdr[4:6]))
	equal(t, []byte("version\r\n"), hdr[8:])
}

func Test_assembleUDPReassemblesAndDropsWrongID(t *testing.T) {
	makeDgram := func(id, seq, total uint16, payload []byte) []byte {
		h := make([]byte, 8+len(payload))
		binary.BigEndian.PutUint16(h[0:2], id)
		binary.BigEndian.PutUint16(h[2:4], seq)
		binary.BigEndian.PutUint16(h[4:6], total)
		copy(h[8:], payload)
		return h
	}
	cn := &recordUDPConn{
		mockConn: newMockConn(),
		datagrams: [][]byte{
			makeDgram(99, 0, 1, []byte("spoof")),
			makeDgram(3, 1, 2, []byte("END\r\n")),
			makeDgram(3, 0, 2, []byte("VALUE k 0 3\r\nabc\r\n")),
		},
	}
	got, err := assembleUDP(cn, 3, udpFragmentCap(0))
	noError(t, err)
	defer putUDPBuf(got)
	equal(t, []byte("VALUE k 0 3\r\nabc\r\nEND\r\n"), *got)
}

func Test_enableUDPFramingOnlyOnUDP(t *testing.T) {
	c := &Client{}
	req := buildRequest([]byte("version"), nil, []byte("version\r\n"))
	resp := buildLimitedLineResponse(1)
	c.enableUDPFraming(newAddrWeighted("tcp", "127.0.0.1:11211", 0), req, resp)
	isFalse(t, req.udpEnabled)
	isFalse(t, resp.udpEnabled)

	c.enableUDPFraming(newAddrWeighted("udp", "127.0.0.1:11211", 0), req, resp)
	isTrue(t, req.udpEnabled)
	isTrue(t, resp.udpEnabled)
	notZero(t, req.udpReqID)
	equal(t, req.udpReqID, resp.udpReqID)
}

func Test_nextUDPRequestIDChanges(t *testing.T) {
	a := nextUDPRequestID()
	b := nextUDPRequestID()
	if a == b {
		t.Fatalf("request ids collided: %d", a)
	}
}

func Test_nextUDPRequestIDSkipsZero(t *testing.T) {
	for range 32 {
		notZero(t, nextUDPRequestID())
	}
}

func Test_assembleUDPAcceptsNineFragments(t *testing.T) {
	makeDgram := func(id, seq, total uint16, payload []byte) []byte {
		h := make([]byte, 8+len(payload))
		binary.BigEndian.PutUint16(h[0:2], id)
		binary.BigEndian.PutUint16(h[2:4], seq)
		binary.BigEndian.PutUint16(h[4:6], total)
		copy(h[8:], payload)
		return h
	}
	const total uint16 = 9
	datagrams := make([][]byte, 0, total)
	var want []byte
	for i := range total {
		p := []byte{byte('A' + i)}
		want = append(want, p...)
		datagrams = append(datagrams, makeDgram(3, i, total, p))
	}
	cn := &recordUDPConn{mockConn: newMockConn(), datagrams: datagrams}
	got, err := assembleUDP(cn, 3, udpFragmentCap(0))
	noError(t, err)
	defer putUDPBuf(got)
	equal(t, want, *got)
}

func Test_assembleUDPRejectsReserved(t *testing.T) {
	h := make([]byte, 10)
	binary.BigEndian.PutUint16(h[0:2], 3)
	binary.BigEndian.PutUint16(h[4:6], 1)
	binary.BigEndian.PutUint16(h[6:8], 1)
	cn := &recordUDPConn{mockConn: newMockConn(), datagrams: [][]byte{h}}
	_, err := assembleUDP(cn, 3, udpFragmentCap(0))
	isErrorIs(t, err, ErrMalformedResponse)
}

func Test_assembleUDPRejectsFragmentConflict(t *testing.T) {
	makeDgram := func(payload []byte) []byte {
		h := make([]byte, 8+len(payload))
		binary.BigEndian.PutUint16(h[0:2], 3)
		binary.BigEndian.PutUint16(h[4:6], 2)
		copy(h[8:], payload)
		return h
	}
	cn := &recordUDPConn{
		mockConn:  newMockConn(),
		datagrams: [][]byte{makeDgram([]byte("A")), makeDgram([]byte("B"))},
	}
	_, err := assembleUDP(cn, 3, udpFragmentCap(0))
	isErrorIs(t, err, ErrMalformedResponse)
}

func Test_udpFragmentCap(t *testing.T) {
	want := (udpAssemblyBudget(0) + udpFragmentPayloadSize - 1) / udpFragmentPayloadSize
	equal(t, want, udpFragmentCap(0))
	equal(t, want, udpFragmentCap(defaultMaxValueBytes))
	greater(t, udpFragmentCap(4<<20), want)
	h := make([]byte, 10)
	binary.BigEndian.PutUint16(h[0:2], 3)
	binary.BigEndian.PutUint16(h[4:6], 65)
	cn := &recordUDPConn{mockConn: newMockConn(), datagrams: [][]byte{h}}
	_, err := assembleUDP(cn, 3, 64)
	isErrorIs(t, err, ErrMalformedResponse)
}

func Test_udpAssemblyByteBudget(t *testing.T) {
	makeDgram := func(seq, tot uint16, payload []byte) []byte {
		h := make([]byte, udpHeaderSize+len(payload))
		binary.BigEndian.PutUint16(h[0:2], 3)
		binary.BigEndian.PutUint16(h[2:4], seq)
		binary.BigEndian.PutUint16(h[4:6], tot)
		copy(h[udpHeaderSize:], payload)
		return h
	}
	payload := bytes.Repeat([]byte("x"), 100)
	cn := &recordUDPConn{
		mockConn: newMockConn(),
		datagrams: [][]byte{
			makeDgram(0, 3, payload),
			makeDgram(1, 3, payload),
			makeDgram(2, 3, payload),
		},
	}
	_, _, err := assembleUDPPayload(cn, 3, 8, 150, true)
	isErrorIs(t, err, ErrMalformedResponse)
}

func Test_nextUDPRequestIDWrapBumpsEpoch(t *testing.T) {
	prev := udpRequestEpoch.Load()
	udpRequestID.Store(math.MaxUint16)
	id := nextUDPRequestID()
	equal(t, uint16(1), id)
	equal(t, prev+1, udpRequestEpoch.Load())
}

func TestVersionAllRotatesUDPBeforeWrappedIDReceive(t *testing.T) {
	server, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	noError(t, err)
	t.Cleanup(func() { _ = server.Close() })

	oldID := udpRequestID.Load()
	oldEpoch := udpRequestEpoch.Load()
	t.Cleanup(func() {
		udpRequestID.Store(oldID)
		udpRequestEpoch.Store(oldEpoch)
	})
	udpRequestID.Store(41)

	firstDone := make(chan *net.UDPAddr, 1)
	secondDone := make(chan *net.UDPAddr, 1)
	go func() {
		buf := make([]byte, 256)
		_ = server.SetReadDeadline(time.Now().Add(5 * time.Second))
		n, first, readErr := server.ReadFromUDP(buf)
		if readErr != nil || n < udpHeaderSize {
			return
		}
		reply := make([]byte, udpHeaderSize+len("VERSION first\r\n"))
		copy(reply[:2], buf[:2])
		binary.BigEndian.PutUint16(reply[4:6], 1)
		copy(reply[udpHeaderSize:], "VERSION first\r\n")
		_, _ = server.WriteToUDP(reply, first)

		stale := make([]byte, udpHeaderSize+len("VERSION stale\r\n"))
		binary.BigEndian.PutUint16(stale[:2], 1)
		binary.BigEndian.PutUint16(stale[4:6], 1)
		copy(stale[udpHeaderSize:], "VERSION stale\r\n")
		_, _ = server.WriteToUDP(stale, first)
		firstDone <- first

		n, second, readErr := server.ReadFromUDP(buf)
		if readErr != nil || n < udpHeaderSize {
			return
		}
		reply = make([]byte, udpHeaderSize+len("VERSION second\r\n"))
		copy(reply[:2], buf[:2])
		binary.BigEndian.PutUint16(reply[4:6], 1)
		copy(reply[udpHeaderSize:], "VERSION second\r\n")
		_, _ = server.WriteToUDP(reply, second)
		secondDone <- second
	}()

	client, err := New(Config{Servers: []Server{{
		Network: "udp",
		Address: server.LocalAddr().String(),
	}}})
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var versions []NodeVersion
	noError(t, client.VersionAll(ctx, &versions))
	equal(t, []byte("first"), versions[0].Version)
	first := <-firstDone

	udpRequestID.Store(math.MaxUint16)
	versions = versions[:0]
	noError(t, client.VersionAll(ctx, &versions))
	equal(t, []byte("second"), versions[0].Version)
	second := <-secondDone
	if first.Port == second.Port {
		t.Fatalf("wrapped request reused UDP socket port %d", first.Port)
	}
}

func Test_udpRequestAllowedAllocs(t *testing.T) {
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

func Test_udpMetaRawAllowedAllocs(t *testing.T) {
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

func Test_udpMetaRawAllowedRejectsSideEffect(t *testing.T) {
	for _, flag := range []string{"N30", "R30", "T30", "Eabc"} {
		isErrorIs(t, udpMetaRawAllowed([]byte("mg k "+flag+"\r\n")), ErrNotSupported)
	}
	isErrorIs(t, udpMetaRawAllowed([]byte("ms k\r\n")), ErrNotSupported)
	noError(t, udpMetaRawAllowed([]byte("mg k v\r\n")))
	noError(t, udpMetaRawAllowed([]byte("mn\r\n")))
	noError(t, udpMetaRawAllowed([]byte("me k\r\n")))
}

func Test_sendUDPRejectsOversized(t *testing.T) {
	req := buildRequest([]byte("get"), []byte("k"), make([]byte, maxUDPDatagram))
	req.udpEnabled = true
	req.udpReqID = 1
	err := req.sendUDP(&recordUDPConn{mockConn: newMockConn()})
	isErrorIs(t, err, ErrNotSupported)
}

func Test_assembleUDPThreeFragmentsInOrderAllocs(t *testing.T) {
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

func Test_assembleUDPPayloadInPlaceTot1(t *testing.T) {
	payload := []byte("VALUE k 0 1\r\nx\r\nEND\r\n")
	d := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint16(d[0:2], 3)
	binary.BigEndian.PutUint16(d[4:6], 1)
	copy(d[8:], payload)
	cn := &recordUDPConn{mockConn: newMockConn(), datagrams: [][]byte{d}}
	box, view, err := assembleUDPPayload(cn, 3, udpFragmentCap(0), 0, false)
	noError(t, err)
	if box != nil {
		t.Fatal("tot=1 in-place must not take a pool box")
	}
	equal(t, payload, view)
	if &view[0] != &d[8] {
		t.Fatal("tot=1 in-place must view the datagram")
	}
}

func Test_assembleUDPSingleFragmentAllocs(t *testing.T) {
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

func Test_sendUDPAllocs(t *testing.T) {
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
