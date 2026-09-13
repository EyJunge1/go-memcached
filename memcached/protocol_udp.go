package memcached

import (
	"bytes"
	crand "crypto/rand"
	"encoding/binary"
	"fmt"
	"sync/atomic"
)

const (
	udpHeaderSize          = 8
	maxUDPDatagram         = 65507
	minUDPFragments        = 2048
	maxUDPFragments        = 65535
	udpFragmentPayloadSize = 1392
)

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
	var b [2]byte
	if _, err := crand.Read(b[:]); err == nil {
		id := binary.BigEndian.Uint16(b[:])
		if id != 0 {
			return id
		}
	}
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

func udpRequestAllowed(req *request) error {
	if req == nil {
		return fmt.Errorf("udp: %w", ErrNotSupported)
	}
	switch string(req.cmd) {
	case "get", "gets", "version", "me":
		return nil
	case "mg", "mn":
		return udpMetaRawAllowed(req.raw)
	default:
		return fmt.Errorf("udp %s: %w", req.cmd, ErrNotSupported)
	}
}

func udpMetaRawAllowed(raw []byte) error {
	for _, line := range bytes.Split(raw, []byte("\r\n")) {
		fields := bytes.Fields(line)
		if len(fields) == 0 {
			continue
		}
		switch string(fields[0]) {
		case "mn", "me":
			continue
		case "mg":
			if mgLineHasSideEffect(fields) {
				return fmt.Errorf("udp meta side-effect: %w", ErrNotSupported)
			}
		default:
			return fmt.Errorf("udp %s: %w", fields[0], ErrNotSupported)
		}
	}
	return nil
}

func mgLineHasSideEffect(fields [][]byte) bool {
	if len(fields) < 3 {
		return false
	}
	for _, f := range fields[2:] {
		if len(f) == 0 {
			continue
		}
		switch f[0] {
		case 'N', 'R', 'T', 'E':
			return true
		}
	}
	return false
}

func idempotentReadCommand(cmd []byte) bool {
	switch string(cmd) {
	case "get", "gets", "version", "mg", "mn", "me":
		return true
	default:
		return false
	}
}

// sendUDP sends the request to the memcached server.
// https://github.com/memcached/memcached/blob/master/doc/protocol.txt#L1875-L1914
func (req *request) sendUDP(rr wireConn) (err error) {
	payload := req.wire()
	if udpHeaderSize+len(payload) > maxUDPDatagram {
		return fmt.Errorf("udp datagram too large: %w", ErrNotSupported)
	}
	header := make([]byte, udpHeaderSize)
	binary.BigEndian.PutUint16(header[0:2], req.udpReqID)
	binary.BigEndian.PutUint16(header[2:4], 0)
	binary.BigEndian.PutUint16(header[4:6], 1)
	datagram := make([]byte, 0, udpHeaderSize+len(payload))
	datagram = append(datagram, header...)
	datagram = append(datagram, payload...)

	if _, err = rr.Write(datagram); err != nil {
		return err
	}
	return rr.Flush()
}

func udpFragmentCap(maxValue int) int {
	if maxValue <= 0 {
		maxValue = defaultMaxValueBytes
	}
	n := (maxValue + udpFragmentPayloadSize - 1) / udpFragmentPayloadSize
	if n < minUDPFragments {
		n = minUDPFragments
	}
	if n > maxUDPFragments {
		n = maxUDPFragments
	}
	return n
}

func assembleUDP(rr wireConn, wantID uint16, maxFragments int) ([]byte, error) {
	if maxFragments <= 0 {
		maxFragments = udpFragmentCap(0)
	}
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
		if binary.BigEndian.Uint16(dgram[6:8]) != 0 {
			return nil, fmt.Errorf("udp reserved must be 0: %w", ErrMalformedResponse)
		}
		seq := int(binary.BigEndian.Uint16(dgram[2:4]))
		tot := int(binary.BigEndian.Uint16(dgram[4:6]))
		if tot <= 0 || tot > maxFragments {
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
		payload := dgram[udpHeaderSize:]
		if slots[seq] != nil {
			if !bytes.Equal(slots[seq], payload) {
				return nil, fmt.Errorf("udp fragment conflict: %w", ErrMalformedResponse)
			}
			continue
		}
		slots[seq] = payload
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
