package memcached

import (
	"bytes"
	crand "crypto/rand"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
)

const (
	udpHeaderSize          = 8
	maxUDPDatagram         = 65507
	minUDPFragments        = 2048
	maxUDPFragments        = 65535
	udpFragmentPayloadSize = 1392
	udpPooledBuf           = 8 << 10
	udpMaxPooled           = 1 << 16
)

var (
	udpRequestID atomic.Uint32
	udpBufPool   = sync.Pool{New: func() any {
		b := make([]byte, 0, udpPooledBuf)
		return &b
	}}
)

// init seeds the UDP request-id counter.
func init() {
	seedUDPRequestID()
}

// seedUDPRequestID seeds the UDP request-id counter from crypto/rand.
func seedUDPRequestID() {
	var b [2]byte
	if _, err := crand.Read(b[:]); err != nil {
		return
	}
	start := binary.BigEndian.Uint16(b[:])
	if start == 0 {
		start = 1
	}
	udpRequestID.Store(uint32(start))
}

func takeUDPBuf() *[]byte {
	p := udpBufPool.Get().(*[]byte)
	*p = (*p)[:0]
	return p
}

func putUDPBuf(p *[]byte) {
	if p == nil {
		return
	}
	if cap(*p) > udpMaxPooled {
		return
	}
	*p = (*p)[:0]
	udpBufPool.Put(p)
}

// nextUDPRequestID returns a non-zero 16-bit id. Zero is reserved on the wire.
func nextUDPRequestID() uint16 {
	for {
		id := uint16(udpRequestID.Add(1) & 0xffff) //nolint:gosec // G115: protocol field is 16-bit
		if id != 0 {
			return id
		}
	}
}

// isUDPNetwork reports whether network is udp, udp4, or udp6.
func isUDPNetwork(network string) bool {
	return network == "udp" || network == "udp4" || network == "udp6"
}

// udpRequestAllowed rejects commands that are unsafe on UDP.
func udpRequestAllowed(req *request) error {
	if req == nil {
		return fmt.Errorf("udp: %w", ErrNotSupported)
	}
	switch {
	case bytes.Equal(req.cmd, _GetBytes), bytes.Equal(req.cmd, _GetsBytes),
		bytes.Equal(req.cmd, _VersionCmdBytes), bytes.Equal(req.cmd, _MeBytes):
		return nil
	case bytes.Equal(req.cmd, _MgBytes), bytes.Equal(req.cmd, _MnBytes):
		return udpMetaRawAllowed(req.raw)
	default:
		return fmt.Errorf("udp %s: %w", req.cmd, ErrNotSupported)
	}
}

// udpMetaRawAllowed rejects meta lines other than mn, me, or side-effect-free mg.
func udpMetaRawAllowed(raw []byte) error {
	for len(raw) > 0 {
		line := raw
		if i := bytes.IndexByte(raw, '\n'); i >= 0 {
			line = raw[:i]
			raw = raw[i+1:]
		} else {
			raw = nil
		}
		if n := len(line); n > 0 && line[n-1] == '\r' {
			line = line[:n-1]
		}
		cmd, rest := nextASCIIToken(line)
		if len(cmd) == 0 {
			continue
		}
		switch {
		case bytes.Equal(cmd, _MnBytes), bytes.Equal(cmd, _MeBytes):
			continue
		case bytes.Equal(cmd, _MgBytes):
			if mgRestHasSideEffect(rest) {
				return fmt.Errorf("udp meta side-effect: %w", ErrNotSupported)
			}
		default:
			return fmt.Errorf("udp %s: %w", cmd, ErrNotSupported)
		}
	}
	return nil
}

func nextASCIIToken(line []byte) (token, rest []byte) {
	i := 0
	for i < len(line) && (line[i] == ' ' || line[i] == '\t') {
		i++
	}
	j := i
	for j < len(line) && line[j] != ' ' && line[j] != '\t' {
		j++
	}
	return line[i:j], line[j:]
}

// mgRestHasSideEffect reports whether mg flags after the verb include N, R, T, or E.
func mgRestHasSideEffect(rest []byte) bool {
	_, rest = nextASCIIToken(rest)
	for {
		var flag []byte
		flag, rest = nextASCIIToken(rest)
		if len(flag) == 0 {
			return false
		}
		switch flag[0] {
		case 'N', 'R', 'T', 'E':
			return true
		}
	}
}

// idempotentReadCommand reports whether cmd is a UDP-safe read.
func idempotentReadCommand(cmd []byte) bool {
	return bytes.Equal(cmd, _GetBytes) ||
		bytes.Equal(cmd, _GetsBytes) ||
		bytes.Equal(cmd, _VersionCmdBytes) ||
		bytes.Equal(cmd, _MgBytes) ||
		bytes.Equal(cmd, _MnBytes) ||
		bytes.Equal(cmd, _MeBytes)
}

// sendUDP writes one 8-byte framed datagram (protocol.txt UDP section).
func (req *request) sendUDP(rr wireConn) error {
	need := udpHeaderSize + len(req.raw) + len(req.body) + len(req.tail)
	if need > maxUDPDatagram {
		return fmt.Errorf("udp datagram too large: %w", ErrNotSupported)
	}
	if c, ok := rr.(*conn); ok {
		if c.isClosed() {
			return errConnClosed
		}
		buf := c.udpOut
		if cap(buf) < need {
			buf = make([]byte, need)
			c.udpOut = buf
		} else {
			buf = buf[:need]
			c.udpOut = buf
		}
		fillUDPDatagram(buf, req)
		_, err := c.raw.Write(buf)
		return err
	}
	box := takeUDPBuf()
	if cap(*box) < need {
		*box = make([]byte, need)
	} else {
		*box = (*box)[:need]
	}
	fillUDPDatagram(*box, req)
	_, err := rr.Write(*box)
	putUDPBuf(box)
	if err != nil {
		return err
	}
	return rr.Flush()
}

func fillUDPDatagram(buf []byte, req *request) {
	binary.BigEndian.PutUint16(buf[0:2], req.udpReqID)
	binary.BigEndian.PutUint16(buf[2:4], 0)
	binary.BigEndian.PutUint16(buf[4:6], 1)
	buf[6] = 0
	buf[7] = 0
	off := udpHeaderSize
	off += copy(buf[off:], req.raw)
	off += copy(buf[off:], req.body)
	copy(buf[off:], req.tail)
}

// udpFragmentCap returns how many UDP fragments a maxValue reply may use.
func udpFragmentCap(maxValue int) int {
	if maxValue <= 0 {
		maxValue = defaultMaxValueBytes
	}
	n := min(max((maxValue+udpFragmentPayloadSize-1)/udpFragmentPayloadSize, minUDPFragments), maxUDPFragments)
	return n
}

// assembleUDP reassembles datagrams with wantID into a pooled payload box.
// The returned box is only valid until putUDPBuf; recv keeps it until release.
func assembleUDP(rr wireConn, wantID uint16, maxFragments int) (*[]byte, error) {
	box, _, err := assembleUDPPayload(rr, wantID, maxFragments, true)
	return box, err
}

// assembleUDPPayload reassembles wantID. If own is false and the reply is a
// single fragment, payload is a view into the last datagram (valid until the
// next readDatagram). Otherwise payload is backed by the returned box.
func assembleUDPPayload(rr wireConn, wantID uint16, maxFragments int, own bool) (*[]byte, []byte, error) {
	if maxFragments <= 0 {
		maxFragments = udpFragmentCap(0)
	}
	var (
		box     *[]byte
		total   int
		got     int
		offs    []int
		lens    []int
		offStor [32]int
		lenStor [32]int
	)
	for {
		dgram, err := rr.readDatagram()
		if err != nil {
			return udpAssembleFail(box, fmt.Errorf("udp datagram: %w", err))
		}
		if len(dgram) < udpHeaderSize {
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
		payload := dgram[udpHeaderSize:]
		if tot == 1 {
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
			need := tot * udpFragmentPayloadSize
			if cap(*box) < need {
				*box = make([]byte, 0, need)
			} else {
				*box = (*box)[:0]
			}
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
		if lens[seq] >= 0 {
			existing := (*box)[offs[seq] : offs[seq]+lens[seq]]
			if !bytes.Equal(existing, payload) {
				return udpAssembleFail(box, fmt.Errorf("udp fragment conflict: %w", ErrMalformedResponse))
			}
			continue
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
