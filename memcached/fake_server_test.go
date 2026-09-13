package memcached

import (
	"bufio"
	"bytes"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

type fakeItem struct {
	value []byte
	flags uint32
	cas   uint64
}

type fakeMemcached struct {
	ln       net.Listener
	mu       sync.Mutex
	items    map[string]*fakeItem
	cas      uint64
	lastCmd  string
	watchOK  bool
	dumpSel  string
	dumpBusy bool
}

func startFakeMemcached(t *testing.T) *fakeMemcached {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return serveFake(t, ln)
}

func startFakeMemcachedUnix(t *testing.T) (*fakeMemcached, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "memcached.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	return serveFake(t, ln), path
}

func serveFake(t *testing.T, ln net.Listener) *fakeMemcached {
	t.Helper()
	f := &fakeMemcached{ln: ln, items: make(map[string]*fakeItem)}
	go f.serve()
	t.Cleanup(func() { _ = ln.Close() })
	return f
}

func (f *fakeMemcached) Addr() string {
	return f.ln.Addr().String()
}

func (f *fakeMemcached) serve() {
	for {
		cn, err := f.ln.Accept()
		if err != nil {
			return
		}
		go f.handle(cn)
	}
}

func (f *fakeMemcached) handle(cn net.Conn) {
	defer func() { _ = cn.Close() }()
	r := bufio.NewReader(cn)
	w := bufio.NewWriter(cn)

	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			return
		}
		fields := strings.Fields(string(bytes.TrimRight(line, "\r\n")))
		if len(fields) == 0 {
			continue
		}
		f.mu.Lock()
		f.lastCmd = strings.Join(fields, " ")
		f.mu.Unlock()
		switch fields[0] {
		case "quit":
			return
		case "version":
			_, _ = w.WriteString("VERSION fake-1.0\r\n")
			_ = w.Flush()
		case "flush_all":
			f.mu.Lock()
			f.items = make(map[string]*fakeItem)
			f.mu.Unlock()
			_, _ = w.WriteString("OK\r\n")
			_ = w.Flush()
		case "set", "add", "replace", "append", "prepend", "cas":
			if !f.store(fields, r, w) {
				return
			}
		case "delete":
			f.delete(fields, w)
		case "incr", "decr":
			f.arithmetic(fields, w)
		case "mn":
			_, _ = w.WriteString("MN\r\n")
			_ = w.Flush()
		case "ms":
			if !f.metaSet(fields, r, w) {
				return
			}
		case "mg":
			f.metaGet(fields, w)
		case "stats":
			f.stats(fields, w)
		case "verbosity", "cache_memlimit":
			_, _ = w.WriteString("OK\r\n")
			_ = w.Flush()
		case "shutdown":
			_, _ = w.WriteString("OK\r\n")
			_ = w.Flush()
			return
		case "slabs":
			f.slabs(fields, w)
		case "lru":
			_, _ = w.WriteString("OK\r\n")
			_ = w.Flush()
		case "lru_crawler":
			f.lruCrawler(fields, w)
		case "watch":
			f.watch(fields, w, cn)
			return
		case "get", "gets":
			f.retrieve(fields[1:], fields[0] == "gets", w)
		case "gat", "gats":
			if len(fields) < 3 {
				_, _ = w.WriteString("ERROR\r\n")
				_ = w.Flush()
				continue
			}
			f.retrieve(fields[2:], fields[0] == "gats", w)
		default:
			_, _ = w.WriteString("ERROR\r\n")
			_ = w.Flush()
		}
	}
}

type prefixPicker struct {
	addrs []*Addr
}

func (p prefixPicker) Pick(_ []*Addr, _, key []byte) (*Addr, error) {
	if len(p.addrs) == 0 {
		return nil, ErrInvalidAddress
	}
	if len(p.addrs) > 1 && bytes.HasPrefix(key, []byte("b-")) {
		return p.addrs[1], nil
	}
	return p.addrs[0], nil
}

type firstAddrPicker struct{}

func (firstAddrPicker) Pick(addrs []*Addr, _, _ []byte) (*Addr, error) {
	if len(addrs) == 0 {
		return nil, ErrInvalidAddress
	}
	return addrs[0], nil
}

type firstAddrPickBuilder struct{}

func (firstAddrPickBuilder) Build([]*Addr) Picker {
	return firstAddrPicker{}
}

type prefixPickBuilder struct{}

func (prefixPickBuilder) Build(addrs []*Addr) Picker {
	return prefixPicker{addrs: addrs}
}
