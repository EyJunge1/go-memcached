package memcached

import (
	"encoding/binary"
	"net"
	"strings"
	"sync"
	"testing"
)

type fakeUDP struct {
	pc         net.PacketConn
	wg         sync.WaitGroup
	mu         sync.Mutex
	items      map[string][]byte
	lastID     uint16
	spoofFirst bool
	reserved   bool
}

func startFakeMemcachedUDP(t *testing.T) *fakeUDP {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeUDP{pc: pc, items: make(map[string][]byte)}
	f.wg.Add(1)
	go f.serve()
	t.Cleanup(func() {
		_ = pc.Close()
		f.wg.Wait()
	})
	return f
}

func (f *fakeUDP) Addr() string {
	addr := f.pc.LocalAddr()
	if addr == nil {
		return "udp://"
	}
	return "udp://" + addr.String()
}

func (f *fakeUDP) serve() {
	defer f.wg.Done()
	buf := make([]byte, 65535)
	for {
		n, addr, err := f.pc.ReadFrom(buf)
		if err != nil {
			return
		}
		if n < _udpHeaderSize {
			continue
		}
		id := binary.BigEndian.Uint16(buf[0:2])
		payload := string(buf[_udpHeaderSize:n])
		f.mu.Lock()
		f.lastID = id
		spoof := f.spoofFirst
		reserved := f.reserved
		f.spoofFirst = false
		f.mu.Unlock()

		fields := strings.Fields(strings.TrimRight(payload, "\r\n"))
		if len(fields) == 0 {
			continue
		}
		var body string
		switch fields[0] {
		case "version":
			body = "VERSION fake-udp\r\n"
		case "get", "gets":
			if len(fields) < 2 {
				body = "END\r\n"
				break
			}
			f.mu.Lock()
			val, ok := f.items[fields[1]]
			f.mu.Unlock()
			body = "END\r\n"
			if ok {
				body = "VALUE " + fields[1] + " 0 " + itoa(len(val)) + "\r\n" + string(val) + "\r\nEND\r\n"
			}
		case "me":
			if len(fields) < 2 {
				body = "EN\r\n"
				break
			}
			f.mu.Lock()
			_, ok := f.items[fields[1]]
			f.mu.Unlock()
			if !ok {
				body = "EN\r\n"
				break
			}
			body = "ME " + fields[1] + " exp=-1 la=0 cas=1 fetch=no cls=1 size=1\r\n"
		case "mg":
			if len(fields) < 2 {
				body = "EN\r\n"
				break
			}
			f.mu.Lock()
			val, ok := f.items[fields[1]]
			f.mu.Unlock()
			wantValue := false
			for _, tok := range fields[2:] {
				if tok == "v" {
					wantValue = true
				}
			}
			if !ok {
				body = "EN\r\n"
				break
			}
			body = "HD\r\n"
			if wantValue {
				body = "VA " + itoa(len(val)) + "\r\n" + string(val) + "\r\n"
			}
		default:
			body = "ERROR\r\n"
		}
		if spoof {
			_ = f.writeFrame(addr, 1, body)
		}
		var res uint16
		if reserved {
			res = 1
		}
		_ = f.writeFrameReserved(addr, id, res, body)
	}
}

func (f *fakeUDP) writeFrame(addr net.Addr, id uint16, body string) error {
	return f.writeFrameReserved(addr, id, 0, body)
}

func (f *fakeUDP) writeFrameReserved(addr net.Addr, id, reserved uint16, body string) error {
	out := make([]byte, _udpHeaderSize+len(body))
	binary.BigEndian.PutUint16(out[0:2], id)
	binary.BigEndian.PutUint16(out[2:4], 0)
	binary.BigEndian.PutUint16(out[4:6], 1)
	binary.BigEndian.PutUint16(out[6:8], reserved)
	copy(out[_udpHeaderSize:], body)
	_, err := f.pc.WriteTo(out, addr)
	return err
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [16]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
