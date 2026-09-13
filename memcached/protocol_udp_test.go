package memcached

import (
	"encoding/binary"
	"io"
	"testing"
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
	got, err := assembleUDP(cn, 3)
	noError(t, err)
	equal(t, []byte("VALUE k 0 3\r\nabc\r\nEND\r\n"), got)
}

func Test_enableUDPFramingOnlyOnUDP(t *testing.T) {
	c := &Client{}
	req := buildRequest([]byte("version"), nil, []byte("version\r\n"))
	resp := buildLimitedLineResponse(1)
	c.enableUDPFraming(&Addr{Network: "tcp", Address: "127.0.0.1:11211"}, req, resp)
	isFalse(t, req.udpEnabled)
	isFalse(t, resp.udpEnabled)

	c.enableUDPFraming(&Addr{Network: "udp", Address: "127.0.0.1:11211"}, req, resp)
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
	for i := uint16(0); i < total; i++ {
		p := []byte{byte('A' + i)}
		want = append(want, p...)
		datagrams = append(datagrams, makeDgram(3, i, total, p))
	}
	cn := &recordUDPConn{mockConn: newMockConn(), datagrams: datagrams}
	got, err := assembleUDP(cn, 3)
	noError(t, err)
	equal(t, want, got)
}

func Test_assembleUDPRejectsReserved(t *testing.T) {
	h := make([]byte, 10)
	binary.BigEndian.PutUint16(h[0:2], 3)
	binary.BigEndian.PutUint16(h[4:6], 1)
	binary.BigEndian.PutUint16(h[6:8], 1)
	cn := &recordUDPConn{mockConn: newMockConn(), datagrams: [][]byte{h}}
	_, err := assembleUDP(cn, 3)
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
	_, err := assembleUDP(cn, 3)
	isErrorIs(t, err, ErrMalformedResponse)
}

func Test_sendUDPRejectsOversized(t *testing.T) {
	req := buildRequest([]byte("get"), []byte("k"), make([]byte, maxUDPDatagram))
	req.udpEnabled = true
	req.udpReqID = 1
	err := req.sendUDP(&recordUDPConn{mockConn: newMockConn()})
	isErrorIs(t, err, ErrNotSupported)
}
