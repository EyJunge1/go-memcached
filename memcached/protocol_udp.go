package memcached

import (
	crand "crypto/rand"
	"encoding/binary"
	"fmt"
	"sync/atomic"
)

const udpHeaderSize = 8

var udpRequestID atomic.Uint32

func init() {
	seedUDPRequestID()
}

func seedUDPRequestID() {
	var b [2]byte
	if _, err := crand.Read(b[:]); err != nil {
		return
	}
	udpRequestID.Store(uint32(binary.BigEndian.Uint16(b[:])))
}

func nextUDPRequestID() uint16 {
	// 16-bit field; skip 0 so an unset/spoofed header never matches a live request.
	for {
		id := uint16(udpRequestID.Add(1) & 0xffff) //nolint:gosec // G115: protocol field is 16-bit
		if id != 0 {
			return id
		}
	}
}

func isUDPNetwork(network string) bool {
	return network == "udp" || network == "udp4" || network == "udp6"
}

// sendUDP sends the request to the memcached server.
// https://github.com/memcached/memcached/blob/master/doc/protocol.txt#L1875-L1914
func (req *request) sendUDP(rr memcachedConn) (err error) {
	header := make([]byte, udpHeaderSize)
	binary.BigEndian.PutUint16(header[0:2], req.udpReqID)
	binary.BigEndian.PutUint16(header[2:4], 0)
	binary.BigEndian.PutUint16(header[4:6], 1)
	datagram := make([]byte, 0, udpHeaderSize+len(req.raw))
	datagram = append(datagram, header...)
	datagram = append(datagram, req.raw...)

	_, err = rr.Write(datagram)
	return err
}

func assembleUDP(rr memcachedConn, wantID uint16) ([]byte, error) {
	var slots [][]byte
	var total int
	got := 0
	for {
		dgram, err := rr.readDatagram()
		if err != nil {
			return nil, fmt.Errorf("udp datagram: %w", err)
		}
		if len(dgram) < udpHeaderSize {
			return nil, fmt.Errorf("short udp header: %w", ErrMalformedResponse)
		}
		id := binary.BigEndian.Uint16(dgram[0:2])
		if id != wantID {
			continue
		}
		seq := int(binary.BigEndian.Uint16(dgram[2:4]))
		tot := int(binary.BigEndian.Uint16(dgram[4:6]))
		if tot <= 0 {
			return nil, fmt.Errorf("invalid udp total: %w", ErrMalformedResponse)
		}
		if slots == nil {
			slots = make([][]byte, tot)
			total = tot
		} else if tot != total {
			return nil, fmt.Errorf("udp total mismatch: %w", ErrMalformedResponse)
		}
		if seq < 0 || seq >= total {
			return nil, fmt.Errorf("udp sequence out of range: %w", ErrMalformedResponse)
		}
		if slots[seq] != nil {
			continue
		}
		slots[seq] = dgram[udpHeaderSize:]
		got++
		if got == total {
			n := 0
			for _, p := range slots {
				n += len(p)
			}
			out := make([]byte, 0, n)
			for _, p := range slots {
				out = append(out, p...)
			}
			return out, nil
		}
	}
}
