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

func TestSendUDPUsesRunningRequestID(t *testing.T) {
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

func TestAssembleUDPReassemblesAndDropsWrongID(t *testing.T) {
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
	got = requirePtr(t, got)
	defer putUDPBuf(got)
	equal(t, []byte("VALUE k 0 3\r\nabc\r\nEND\r\n"), *got)
}

func TestEnableUDPFramingOnlyOnUDP(t *testing.T) {
	c := &Client{}
	req := buildRequest([]byte("version"), nil, []byte("version\r\n"))
	resp := buildLimitedLineResponse(1)
	c.enableUDPFraming(newAddr("tcp", "127.0.0.1:11211"), req, resp)
	isFalse(t, req.udpEnabled)
	isFalse(t, resp.udpEnabled)

	c.enableUDPFraming(newAddr("udp", "127.0.0.1:11211"), req, resp)
	isTrue(t, req.udpEnabled)
	isTrue(t, resp.udpEnabled)
	notZero(t, req.udpReqID)
	equal(t, req.udpReqID, resp.udpReqID)
}

func TestNextUDPRequestIDChanges(t *testing.T) {
	a := nextUDPRequestID()
	b := nextUDPRequestID()
	if a == b {
		t.Fatalf("request ids collided: %d", a)
	}
}

func TestNextUDPRequestIDSkipsZero(t *testing.T) {
	for range 32 {
		notZero(t, nextUDPRequestID())
	}
}

func TestAssembleUDPAcceptsNineFragments(t *testing.T) {
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
	want := make([]byte, 0, total)
	for i := range total {
		p := []byte{byte('A' + i)}
		want = append(want, p...)
		datagrams = append(datagrams, makeDgram(3, i, total, p))
	}
	cn := &recordUDPConn{mockConn: newMockConn(), datagrams: datagrams}
	got, err := assembleUDP(cn, 3, udpFragmentCap(0))
	noError(t, err)
	got = requirePtr(t, got)
	defer putUDPBuf(got)
	equal(t, want, *got)
}

func TestAssembleUDPRejectsReserved(t *testing.T) {
	h := make([]byte, 10)
	binary.BigEndian.PutUint16(h[0:2], 3)
	binary.BigEndian.PutUint16(h[4:6], 1)
	binary.BigEndian.PutUint16(h[6:8], 1)
	cn := &recordUDPConn{mockConn: newMockConn(), datagrams: [][]byte{h}}
	_, err := assembleUDP(cn, 3, udpFragmentCap(0))
	isErrorIs(t, err, ErrMalformedResponse)
}

func TestAssembleUDPRejectsFragmentConflict(t *testing.T) {
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

func TestUDPFragmentCap(t *testing.T) {
	want := (udpAssemblyBudget(0) + _udpFragmentPayloadSize - 1) / _udpFragmentPayloadSize
	equal(t, want, udpFragmentCap(0))
	equal(t, want, udpFragmentCap(_defaultMaxValueBytes))
	greater(t, udpFragmentCap(4<<20), want)
	h := make([]byte, 10)
	binary.BigEndian.PutUint16(h[0:2], 3)
	binary.BigEndian.PutUint16(h[4:6], 65)
	cn := &recordUDPConn{mockConn: newMockConn(), datagrams: [][]byte{h}}
	_, err := assembleUDP(cn, 3, 64)
	isErrorIs(t, err, ErrMalformedResponse)
}

func TestUDPAssemblyByteBudget(t *testing.T) {
	makeDgram := func(seq, tot uint16, payload []byte) []byte {
		h := make([]byte, _udpHeaderSize+len(payload))
		binary.BigEndian.PutUint16(h[0:2], 3)
		binary.BigEndian.PutUint16(h[2:4], seq)
		binary.BigEndian.PutUint16(h[4:6], tot)
		copy(h[_udpHeaderSize:], payload)
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

func TestNextUDPRequestIDWrapBumpsEpoch(t *testing.T) {
	prev := _udpRequestEpoch.Load()
	_udpRequestID.Store(math.MaxUint16)
	id := nextUDPRequestID()
	equal(t, uint16(1), id)
	equal(t, prev+1, _udpRequestEpoch.Load())
}

func TestVersionAllRotatesUDPBeforeWrappedIDReceive(t *testing.T) {
	server, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	noError(t, err)
	t.Cleanup(func() { _ = server.Close() })

	oldID := _udpRequestID.Load()
	oldEpoch := _udpRequestEpoch.Load()
	t.Cleanup(func() {
		_udpRequestID.Store(oldID)
		_udpRequestEpoch.Store(oldEpoch)
	})
	_udpRequestID.Store(41)

	firstDone := make(chan *net.UDPAddr, 1)
	secondDone := make(chan *net.UDPAddr, 1)
	go func() {
		buf := make([]byte, 256)
		_ = server.SetReadDeadline(time.Now().Add(5 * time.Second))
		n, first, readErr := server.ReadFromUDP(buf)
		if readErr != nil || n < _udpHeaderSize {
			return
		}
		reply := make([]byte, _udpHeaderSize+len("VERSION first\r\n"))
		copy(reply[:2], buf[:2])
		binary.BigEndian.PutUint16(reply[4:6], 1)
		copy(reply[_udpHeaderSize:], "VERSION first\r\n")
		_, _ = server.WriteToUDP(reply, first)

		stale := make([]byte, _udpHeaderSize+len("VERSION stale\r\n"))
		binary.BigEndian.PutUint16(stale[:2], 1)
		binary.BigEndian.PutUint16(stale[4:6], 1)
		copy(stale[_udpHeaderSize:], "VERSION stale\r\n")
		_, _ = server.WriteToUDP(stale, first)
		firstDone <- first

		n, second, readErr := server.ReadFromUDP(buf)
		if readErr != nil || n < _udpHeaderSize {
			return
		}
		reply = make([]byte, _udpHeaderSize+len("VERSION second\r\n"))
		copy(reply[:2], buf[:2])
		binary.BigEndian.PutUint16(reply[4:6], 1)
		copy(reply[_udpHeaderSize:], "VERSION second\r\n")
		_, _ = server.WriteToUDP(reply, second)
		secondDone <- second
	}()

	local := server.LocalAddr()
	if local == nil {
		t.Fatal("nil local addr")
	}
	client := mustNewClient(t, Config{Servers: []Server{{
		Network: "udp",
		Address: local.String(),
	}}})
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	versions := make([]NodeVersion, 0, 1)
	noError(t, client.VersionAll(ctx, &versions))
	versions = requireSlice(t, versions)
	hasLen(t, versions, 1)
	equal(t, []byte("first"), versions[0].Version)
	first := <-firstDone

	_udpRequestID.Store(math.MaxUint16)
	versions = versions[:0]
	noError(t, client.VersionAll(ctx, &versions))
	versions = requireSlice(t, versions)
	hasLen(t, versions, 1)
	equal(t, []byte("second"), versions[0].Version)
	second := <-secondDone
	if first.Port == second.Port {
		t.Fatalf("wrapped request reused UDP socket port %d", first.Port)
	}
}

func TestSendUDPRejectsOversized(t *testing.T) {
	req := buildRequest([]byte("get"), []byte("k"), make([]byte, _maxUDPDatagram))
	req.udpEnabled = true
	req.udpReqID = 1
	err := req.sendUDP(&recordUDPConn{mockConn: newMockConn()})
	isErrorIs(t, err, ErrNotSupported)
}

func TestAssembleUDPPayloadInPlaceTot1(t *testing.T) {
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
	view = requireSlice(t, view)
	equal(t, payload, view)
	if len(view) == 0 || &view[0] != &d[8] {
		t.Fatal("tot=1 in-place must view the datagram")
	}
}
