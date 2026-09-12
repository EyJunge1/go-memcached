package memcached

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

func assembleUDP(rr wireConn, wantID uint16, maxFragments int) (*[]byte, error) {
	const ownPayload = true
	box, _, err := assembleUDPPayload(rr, wantID, maxFragments, 0, ownPayload)
	return box, err
}

// assembleUDPPayload reassembles wantID. If own is false and the reply is a
// single fragment, payload is a view into the last datagram (valid until the
// next readDatagram). Otherwise payload is backed by the returned box.
func assembleUDPPayload(rr wireConn, wantID uint16, maxFragments, maxBytes int, own bool) (*[]byte, []byte, error) {
	if maxFragments <= 0 {
		maxFragments = udpFragmentCap(0)
	}
	if maxBytes <= 0 {
		maxBytes = udpAssemblyBudget(0)
	}
	var (
		box     *[]byte
		total   int
		got     int
		offStor [32]int
		lenStor [32]int
	)
	offs := offStor[:0]
	lens := lenStor[:0]
	for {
		dgram, err := rr.readDatagram()
		if err != nil {
			return udpAssembleFail(box, fmt.Errorf("udp datagram: %w", err))
		}
		if len(dgram) < _udpHeaderSize {
			return udpAssembleFail(box, fmt.Errorf("short udp header: %w", ErrMalformedResponse))
		}
		id := binary.BigEndian.Uint16(dgram[0:2])
		if id != wantID {
			continue
		}
		if binary.BigEndian.Uint16(dgram[6:8]) != 0 {
			return udpAssembleFail(box, fmt.Errorf("udp reserved must be 0: %w", ErrMalformedResponse))
		}
		seq := int(binary.BigEndian.Uint16(dgram[2:4]))
		tot := int(binary.BigEndian.Uint16(dgram[4:6]))
		if tot <= 0 || tot > maxFragments {
			return udpAssembleFail(box, fmt.Errorf("invalid udp total: %w", ErrMalformedResponse))
		}
		if seq < 0 || seq >= tot {
			return udpAssembleFail(box, fmt.Errorf("udp sequence out of range: %w", ErrMalformedResponse))
		}
		payload := dgram[_udpHeaderSize:]
		if tot == 1 {
			if len(payload) > maxBytes {
				return udpAssembleFail(box, fmt.Errorf("udp payload too large: %w", ErrMalformedResponse))
			}
			if !own {
				return nil, payload, nil
			}
			if box == nil {
				box = takeUDPBuf()
			}
			*box = append((*box)[:0], payload...)
			return box, *box, nil
		}
		if box == nil {
			box = takeUDPBuf()
		}
		if total == 0 {
			// The total field is untrusted and says nothing about the actual
			// payload sizes. Grow from received bytes instead of reserving up
			// to several MiB for a tiny or incomplete response.
			*box = (*box)[:0]
			if tot <= len(offStor) {
				offs = offStor[:tot]
				lens = lenStor[:tot]
			} else {
				offs = make([]int, tot)
				lens = make([]int, tot)
			}
			for i := range lens {
				lens[i] = -1
			}
			total = tot
		} else if tot != total {
			return udpAssembleFail(box, fmt.Errorf("udp total mismatch: %w", ErrMalformedResponse))
		}
		if seq >= len(lens) || seq >= len(offs) {
			return udpAssembleFail(box, fmt.Errorf("udp sequence out of range: %w", ErrMalformedResponse))
		}
		if lens[seq] >= 0 {
			existing := (*box)[offs[seq] : offs[seq]+lens[seq]]
			if !bytes.Equal(existing, payload) {
				return udpAssembleFail(box, fmt.Errorf("udp fragment conflict: %w", ErrMalformedResponse))
			}
			continue
		}
		if len(*box)+len(payload) > maxBytes {
			return udpAssembleFail(box, fmt.Errorf("udp assembly budget exceeded: %w", ErrMalformedResponse))
		}
		offs[seq] = len(*box)
		lens[seq] = len(payload)
		*box = append(*box, payload...)
		got++
		if got != total {
			continue
		}
		if udpArrivalInSeq(offs, lens) {
			return box, *box, nil
		}
		ord := takeUDPBuf()
		if cap(*ord) < len(*box) {
			*ord = make([]byte, 0, len(*box))
		}
		for i := 0; i < total; i++ {
			*ord = append(*ord, (*box)[offs[i]:offs[i]+lens[i]]...)
		}
		putUDPBuf(box)
		return ord, *ord, nil
	}
}

func udpAssembleFail(box *[]byte, err error) (*[]byte, []byte, error) {
	putUDPBuf(box)
	return nil, nil, err
}

func udpArrivalInSeq(offs, lens []int) bool {
	pos := 0
	for i := range offs {
		if offs[i] != pos {
			return false
		}
		pos += lens[i]
	}
	return true
}
