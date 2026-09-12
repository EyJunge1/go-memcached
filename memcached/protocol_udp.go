package memcached

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
)

const (
	_udpHeaderSize          = 8
	_maxUDPDatagram         = 65507
	_minUDPFragments        = 1
	_maxUDPFragments        = 65535
	_udpFragmentPayloadSize = 1392
	_udpPooledBuf           = 8 << 10
	_udpMaxPooled           = 1 << 16
	// _udpAssemblyOverhead budgets protocol framing beyond the value body.
	_udpAssemblyOverhead = 4 << 10
)

// Process-wide UDP request-id space (memcached UDP header is 16-bit).
// Shared deliberately across Client instances so concurrent clients do not
// collide on the same local port/socket path; not per-Client mutable state.
var (
	_udpRequestID    atomic.Uint32
	_udpRequestEpoch atomic.Uint64
	_udpSeedOnce     sync.Once
	_udpBufPool      = sync.Pool{New: func() any {
		b := make([]byte, 0, _udpPooledBuf)
		return &b
	}}
)

// seedUDPRequestID seeds the UDP request-id counter from crypto/rand.
// CompareAndSwap keeps an explicit test Store (e.g. wrap-around setup) intact
// when seed runs on the first nextUDPRequestID call after that Store.
func seedUDPRequestID() {
	var b [2]byte
	if _, err := rand.Read(b[:]); err != nil {
		return
	}
	start := binary.BigEndian.Uint16(b[:])
	if start == 0 {
		start = 1
	}
	_udpRequestID.CompareAndSwap(0, uint32(start))
}

func takeUDPBuf() *[]byte {
	p, ok := _udpBufPool.Get().(*[]byte)
	if !ok || p == nil {
		b := make([]byte, 0, _udpPooledBuf)
		p = &b
	}
	*p = (*p)[:0]
	return p
}

func putUDPBuf(p *[]byte) {
	if p == nil {
		return
	}
	if cap(*p) > _udpMaxPooled {
		return
	}
	*p = (*p)[:0]
	_udpBufPool.Put(p)
}

// nextUDPRequestID returns a non-zero 16-bit id. Zero is reserved on the wire.
// Wrapping bumps _udpRequestEpoch so pooled UDP sockets are rotated before the
// recycled id can match a late datagram from the previous epoch.
func nextUDPRequestID() uint16 {
	_udpSeedOnce.Do(seedUDPRequestID)
	for {
		// 16-bit wire field: wrap via mask (zero is reserved and skipped).
		id := uint16(_udpRequestID.Add(1) & 0xffff)
		if id != 0 {
			return id
		}
		_udpRequestEpoch.Add(1)
	}
}

// udpAssemblyBudget is the maximum reassembled payload accepted for maxValue.
func udpAssemblyBudget(maxValue int) int {
	if maxValue <= 0 {
		maxValue = _defaultMaxValueBytes
	}
	return maxValue + _udpAssemblyOverhead
}

// udpFragmentCap returns how many UDP fragments a maxValue reply may use.
func udpFragmentCap(maxValue int) int {
	budget := udpAssemblyBudget(maxValue)
	n := max((budget+_udpFragmentPayloadSize-1)/_udpFragmentPayloadSize, _minUDPFragments)
	return min(n, _maxUDPFragments)
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
		raw = nil
		if i := bytes.IndexByte(line, '\n'); i >= 0 {
			raw = line[i+1:]
			line = line[:i]
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
	need := _udpHeaderSize + len(req.raw) + len(req.body) + len(req.tail)
	if need > _maxUDPDatagram {
		return fmt.Errorf("udp datagram too large: %w", ErrNotSupported)
	}
	if c, ok := rr.(*conn); ok {
		if c.isClosed() {
			return errConnClosed
		}
		buf := c.udpOut
		if cap(buf) < need {
			buf = make([]byte, need)
		}
		buf = buf[:need]
		c.udpOut = buf
		fillUDPDatagram(buf, req)
		_, err := c.raw.Write(buf)
		return err
	}
	box := takeUDPBuf()
	defer putUDPBuf(box)
	if cap(*box) < need {
		*box = make([]byte, need)
	}
	*box = (*box)[:need]
	fillUDPDatagram(*box, req)
	if _, err := rr.Write(*box); err != nil {
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
	off := _udpHeaderSize
	off += copy(buf[off:], req.raw)
	off += copy(buf[off:], req.body)
	copy(buf[off:], req.tail)
}

// assembleUDP reassembles datagrams with wantID into a pooled payload box.
