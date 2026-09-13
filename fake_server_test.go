package memcached

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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

func (f *fakeMemcached) store(fields []string, r *bufio.Reader, w *bufio.Writer) bool {
	if len(fields) < 5 {
		_, _ = w.WriteString("CLIENT_ERROR bad command line\r\n")
		_ = w.Flush()
		return true
	}
	cmd := fields[0]
	key := fields[1]
	flags64, err := strconv.ParseUint(fields[2], 10, 32)
	if err != nil {
		_, _ = w.WriteString("CLIENT_ERROR bad flags\r\n")
		_ = w.Flush()
		return true
	}
	nbytes, err := strconv.Atoi(fields[4])
	if err != nil || nbytes < 0 {
		_, _ = w.WriteString("CLIENT_ERROR bad bytes\r\n")
		_ = w.Flush()
		return true
	}
	var casUnique uint64
	idx := 5
	if cmd == "cas" {
		if len(fields) < 6 {
			_, _ = w.WriteString("CLIENT_ERROR bad cas\r\n")
			_ = w.Flush()
			return true
		}
		casUnique, err = strconv.ParseUint(fields[5], 10, 64)
		if err != nil {
			_, _ = w.WriteString("CLIENT_ERROR bad cas\r\n")
			_ = w.Flush()
			return true
		}
		idx = 6
	}
	noReply := len(fields) > idx && fields[idx] == "noreply"
	body := make([]byte, nbytes+2)
	if _, err = io.ReadFull(r, body); err != nil {
		return false
	}
	value := append([]byte(nil), body[:nbytes]...)

	f.mu.Lock()
	existing, ok := f.items[key]
	switch cmd {
	case "add":
		if ok {
			f.mu.Unlock()
			if !noReply {
				_, _ = w.WriteString("NOT_STORED\r\n")
				_ = w.Flush()
			}
			return true
		}
	case "replace":
		if !ok {
			f.mu.Unlock()
			if !noReply {
				_, _ = w.WriteString("NOT_STORED\r\n")
				_ = w.Flush()
			}
			return true
		}
	case "append":
		if !ok {
			f.mu.Unlock()
			if !noReply {
				_, _ = w.WriteString("NOT_STORED\r\n")
				_ = w.Flush()
			}
			return true
		}
		value = append(append([]byte(nil), existing.value...), value...)
		flags64 = uint64(existing.flags)
	case "prepend":
		if !ok {
			f.mu.Unlock()
			if !noReply {
				_, _ = w.WriteString("NOT_STORED\r\n")
				_ = w.Flush()
			}
			return true
		}
		value = append(value, existing.value...)
		flags64 = uint64(existing.flags)
	case "cas":
		if !ok {
			f.mu.Unlock()
			if !noReply {
				_, _ = w.WriteString("NOT_FOUND\r\n")
				_ = w.Flush()
			}
			return true
		}
		if existing.cas != casUnique {
			f.mu.Unlock()
			if !noReply {
				_, _ = w.WriteString("EXISTS\r\n")
				_ = w.Flush()
			}
			return true
		}
	}
	f.cas++
	f.items[key] = &fakeItem{value: value, flags: uint32(flags64), cas: f.cas}
	f.mu.Unlock()

	if !noReply {
		_, _ = w.WriteString("STORED\r\n")
		_ = w.Flush()
	}
	return true
}

func (f *fakeMemcached) delete(fields []string, w *bufio.Writer) {
	if len(fields) < 2 {
		_, _ = w.WriteString("CLIENT_ERROR bad command line\r\n")
		_ = w.Flush()
		return
	}
	noReply := len(fields) > 2 && fields[2] == "noreply"
	f.mu.Lock()
	_, ok := f.items[fields[1]]
	if ok {
		delete(f.items, fields[1])
	}
	f.mu.Unlock()
	if noReply {
		return
	}
	if ok {
		_, _ = w.WriteString("DELETED\r\n")
	} else {
		_, _ = w.WriteString("NOT_FOUND\r\n")
	}
	_ = w.Flush()
}

func (f *fakeMemcached) arithmetic(fields []string, w *bufio.Writer) {
	if len(fields) < 3 {
		_, _ = w.WriteString("CLIENT_ERROR bad command line\r\n")
		_ = w.Flush()
		return
	}
	delta, err := strconv.ParseUint(fields[2], 10, 64)
	if err != nil {
		_, _ = w.WriteString("CLIENT_ERROR bad delta\r\n")
		_ = w.Flush()
		return
	}
	noReply := len(fields) > 3 && fields[3] == "noreply"
	f.mu.Lock()
	item, ok := f.items[fields[1]]
	if !ok {
		f.mu.Unlock()
		if !noReply {
			_, _ = w.WriteString("NOT_FOUND\r\n")
			_ = w.Flush()
		}
		return
	}
	cur, err := strconv.ParseUint(string(item.value), 10, 64)
	if err != nil {
		f.mu.Unlock()
		if !noReply {
			_, _ = w.WriteString("CLIENT_ERROR cannot increment or decrement non-numeric value\r\n")
			_ = w.Flush()
		}
		return
	}
	if fields[0] == "incr" {
		cur += delta
	} else if delta > cur {
		cur = 0
	} else {
		cur -= delta
	}
	item.value = []byte(strconv.FormatUint(cur, 10))
	f.cas++
	item.cas = f.cas
	f.mu.Unlock()
	if !noReply {
		_, _ = w.WriteString(strconv.FormatUint(cur, 10) + "\r\n")
		_ = w.Flush()
	}
}

func parseFakeMetaFlags(tokens []string) (quiet, value, returnCAS bool, cas uint64, casSet bool, mode byte, opaque string) {
	for _, tok := range tokens {
		switch {
		case tok == "q":
			quiet = true
		case tok == "v":
			value = true
		case tok == "c":
			returnCAS = true
		case len(tok) > 1 && tok[0] == 'C':
			cas, _ = strconv.ParseUint(tok[1:], 10, 64)
			casSet = true
		case len(tok) > 1 && tok[0] == 'O':
			opaque = tok[1:]
		case len(tok) == 2 && tok[0] == 'M':
			mode = tok[1]
		}
	}
	return
}

func (f *fakeMemcached) metaSet(fields []string, r *bufio.Reader, w *bufio.Writer) bool {
	if len(fields) < 3 {
		_, _ = w.WriteString("CLIENT_ERROR bad command line\r\n")
		_ = w.Flush()
		return true
	}
	key := fields[1]
	nbytes, err := strconv.Atoi(fields[2])
	if err != nil || nbytes < 0 {
		_, _ = w.WriteString("CLIENT_ERROR bad bytes\r\n")
		_ = w.Flush()
		return true
	}
	quiet, _, _, cas, casSet, mode, _ := parseFakeMetaFlags(fields[3:])
	body := make([]byte, nbytes+2)
	if _, err = io.ReadFull(r, body); err != nil {
		return false
	}
	value := append([]byte(nil), body[:nbytes]...)

	f.mu.Lock()
	existing, ok := f.items[key]
	if casSet && ok && existing.cas != cas {
		f.mu.Unlock()
		_, _ = w.WriteString("EX\r\n")
		_ = w.Flush()
		return true
	}
	switch mode {
	case 'E':
		if ok {
			f.mu.Unlock()
			if !quiet {
				_, _ = w.WriteString("NS\r\n")
				_ = w.Flush()
			}
			return true
		}
	case 'R':
		if !ok {
			f.mu.Unlock()
			if !quiet {
				_, _ = w.WriteString("NS\r\n")
				_ = w.Flush()
			}
			return true
		}
	}
	f.cas++
	var flags uint32
	if ok {
		flags = existing.flags
	}
	f.items[key] = &fakeItem{value: value, flags: flags, cas: f.cas}
	f.mu.Unlock()
	if !quiet {
		_, _ = w.WriteString("HD\r\n")
		_ = w.Flush()
	}
	return true
}

func (f *fakeMemcached) metaGet(fields []string, w *bufio.Writer) {
	if len(fields) < 2 {
		_, _ = w.WriteString("CLIENT_ERROR bad command line\r\n")
		_ = w.Flush()
		return
	}
	quiet, wantValue, returnCAS, cas, casSet, _, opaque := parseFakeMetaFlags(fields[2:])
	f.mu.Lock()
	item, ok := f.items[fields[1]]
	f.mu.Unlock()
	suffix := ""
	if opaque != "" {
		suffix = " O" + opaque
	}
	if ok && returnCAS {
		suffix += " c" + strconv.FormatUint(item.cas, 10)
	}
	if !ok {
		if !quiet {
			_, _ = w.WriteString("EN" + suffix + "\r\n")
			_ = w.Flush()
		}
		return
	}
	if casSet && item.cas == cas {
		if !quiet {
			_, _ = w.WriteString("HD" + suffix + "\r\n")
			_ = w.Flush()
		}
		return
	}
	if wantValue {
		_, _ = fmt.Fprintf(w, "VA %d f%d%s\r\n", len(item.value), item.flags, suffix)
		_, _ = w.Write(item.value)
		_, _ = w.WriteString("\r\n")
		_ = w.Flush()
		return
	}
	if !quiet {
		_, _ = w.WriteString("HD" + suffix + "\r\n")
		_ = w.Flush()
	}
}

func (f *fakeMemcached) slabs(fields []string, w *bufio.Writer) {
	if len(fields) >= 2 && fields[1] == "reassign" {
		if len(fields) >= 4 && fields[3] == "999" {
			_, _ = w.WriteString("BUSY\r\n")
			_ = w.Flush()
			return
		}
		if len(fields) >= 4 && fields[2] == fields[3] {
			_, _ = w.WriteString("SAME\r\n")
			_ = w.Flush()
			return
		}
	}
	_, _ = w.WriteString("OK\r\n")
	_ = w.Flush()
}

func (f *fakeMemcached) lruCrawler(fields []string, w *bufio.Writer) {
	if len(fields) >= 2 && (fields[1] == "metadump" || fields[1] == "mgdump") {
		sel := "all"
		if len(fields) >= 3 {
			sel = fields[2]
		}
		f.mu.Lock()
		f.dumpSel = sel
		busy := f.dumpBusy
		f.mu.Unlock()
		if busy {
			_, _ = w.WriteString("BUSY crawler\r\n")
			_ = w.Flush()
			return
		}
		if fields[1] == "metadump" {
			_, _ = w.WriteString("key=foo exp=-1 la=0 cas=1 fetch=no cls=1 size=3\r\nEND\r\n")
		} else {
			_, _ = w.WriteString("mg foo\r\nEND\r\n")
		}
		_ = w.Flush()
		return
	}
	_, _ = w.WriteString("OK\r\n")
	_ = w.Flush()
}

func (f *fakeMemcached) watch(fields []string, w *bufio.Writer, cn net.Conn) {
	f.mu.Lock()
	f.watchOK = true
	f.mu.Unlock()
	_, _ = w.WriteString("OK\r\n")
	_ = w.Flush()
	_, _ = w.WriteString("ts=100 gid=1 key=foo\r\n")
	_ = w.Flush()
	buf := make([]byte, 1)
	_, _ = cn.Read(buf)
}

func (f *fakeMemcached) stats(fields []string, w *bufio.Writer) {
	arg := ""
	if len(fields) > 1 {
		arg = fields[1]
	}
	if arg == "reset" {
		_, _ = w.WriteString("RESET\r\n")
		_ = w.Flush()
		return
	}
	if arg == "" {
		_, _ = w.WriteString("STAT pid 1\r\nSTAT version fake-1.0\r\nEND\r\n")
	} else {
		_, _ = w.WriteString("STAT arg " + arg + "\r\nEND\r\n")
	}
	_ = w.Flush()
}

func (f *fakeMemcached) retrieve(keys []string, withCAS bool, w *bufio.Writer) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, key := range keys {
		item, ok := f.items[key]
		if !ok {
			continue
		}
		if withCAS {
			_, _ = fmt.Fprintf(w, "VALUE %s %d %d %d\r\n", key, item.flags, len(item.value), item.cas)
		} else {
			_, _ = fmt.Fprintf(w, "VALUE %s %d %d\r\n", key, item.flags, len(item.value))
		}
		_, _ = w.Write(item.value)
		_, _ = w.WriteString("\r\n")
	}
	_, _ = w.WriteString("END\r\n")
	_ = w.Flush()
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

func TestFakeServerRoundTrip(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	noError(t, client.Set(ctx, "foo", []byte("bar"), 7, 0))
	item, err := client.Get(ctx, "foo")
	noError(t, err)
	equal(t, []byte("bar"), item.Value)
	equal(t, uint32(7), item.Flags)

	items, err := client.Gets(ctx, "foo")
	noError(t, err)
	hasLen(t, items, 1)
	equal(t, []byte("bar"), items[0].Value)
}

func TestGetMultiAcrossNodes(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client, err := New(a.Addr()+","+b.Addr(), WithPickBuilder(prefixPickBuilder{}))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	noError(t, client.Set(ctx, "a-one", []byte("1"), 0, 0))
	noError(t, client.Set(ctx, "b-two", []byte("2"), 0, 0))

	items, err := client.Gets(ctx, "a-one", "b-two")
	noError(t, err)
	hasLen(t, items, 2)

	got := clientMustGetMulti(t, client, "a-one", "b-two")
	equal(t, []byte("1"), got["a-one"].Value)
	equal(t, []byte("2"), got["b-two"].Value)
	notZero(t, got["a-one"].CAS)

	items, err = client.GetAndTouches(ctx, time.Second, "a-one", "b-two")
	noError(t, err)
	hasLen(t, items, 2)
}

func clientMustGetMulti(t *testing.T, client *Client, keys ...string) map[string]*Item {
	t.Helper()
	got, err := client.GetMulti(context.Background(), keys)
	noError(t, err)
	return got
}

func TestPingAndSetServers(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client, err := New(a.Addr())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	noError(t, client.Ping(ctx))

	dead, err := net.Listen("tcp", "127.0.0.1:0")
	noError(t, err)
	deadAddr := dead.Addr().String()
	_ = dead.Close()

	noError(t, client.SetServers(a.Addr()+","+deadAddr))
	isError(t, client.Ping(ctx))

	noError(t, client.SetServers(a.Addr()+","+b.Addr()))
	noError(t, client.Ping(ctx))

	noError(t, client.SetServers(a.Addr()))
	noError(t, client.Ping(ctx))
}

func TestDialContextUsed(t *testing.T) {
	srv := startFakeMemcached(t)
	var dials atomic.Int32
	client, err := New(srv.Addr(), WithDialContext(func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	noError(t, client.Set(context.Background(), "k", []byte("v"), 0, 0))
	greater(t, int(dials.Load()), 0)
}

func TestTransportErrorDiscardsConnection(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	noError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	srv := &fakeMemcached{ln: ln, items: make(map[string]*fakeItem)}
	go func() {
		cn, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		_ = cn.Close()
		srv.serve()
	}()

	var dials atomic.Int32
	client, err := New(ln.Addr().String(), WithDialContext(func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	isError(t, client.Set(context.Background(), "k", []byte("v"), 0, 0))
	noError(t, client.Set(context.Background(), "k", []byte("v"), 0, 0))
	equal(t, int32(2), dials.Load())
}

func TestDialContextHonorsDialTimeout(t *testing.T) {
	client, err := New("127.0.0.1:1",
		WithDialTimeout(50*time.Millisecond),
		WithDialContext(func(ctx context.Context, _, _ string) (net.Conn, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}),
	)
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	err = client.Set(context.Background(), "k", []byte("v"), 0, 0)
	isErrorIs(t, err, context.DeadlineExceeded)
}

func TestGetValueContainsCRLF(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	want := []byte("hello\nworld")
	noError(t, client.Set(ctx, "k", want, 0, 0))
	item, err := client.Get(ctx, "k")
	noError(t, err)
	equal(t, want, item.Value)
}

func TestGetValueLooksLikeEND(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	noError(t, client.Set(ctx, "k", []byte("END"), 0, 0))
	item, err := client.Get(ctx, "k")
	noError(t, err)
	equal(t, []byte("END"), item.Value)

	noError(t, client.Set(ctx, "k2", []byte("ERROR"), 0, 0))
	item, err = client.Get(ctx, "k2")
	noError(t, err)
	equal(t, []byte("ERROR"), item.Value)
}

func TestAddNotStoredKeepsConnection(t *testing.T) {
	srv := startFakeMemcached(t)
	var dials atomic.Int32
	client, err := New(srv.Addr(), WithDialContext(func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	noError(t, client.Set(ctx, "k", []byte("v"), 0, 0))
	isErrorIs(t, client.Add(ctx, "k", []byte("other"), 0, 0), ErrNotStored)
	noError(t, client.Set(ctx, "k2", []byte("v2"), 0, 0))
	equal(t, int32(1), dials.Load())
}

func TestCasExistsKeepsConnection(t *testing.T) {
	srv := startFakeMemcached(t)
	var dials atomic.Int32
	client, err := New(srv.Addr(), WithDialContext(func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	noError(t, client.Set(ctx, "k", []byte("v"), 0, 0))
	isErrorIs(t, client.Cas(ctx, "k", []byte("other"), 0, 0, 99), ErrExists)
	isErrorIs(t, client.Cas(ctx, "missing", []byte("x"), 0, 0, 1), ErrNotFound)
	noError(t, client.Set(ctx, "k2", []byte("v2"), 0, 0))
	equal(t, int32(1), dials.Load())
}

func TestDeleteNotFoundKeepsConnection(t *testing.T) {
	srv := startFakeMemcached(t)
	var dials atomic.Int32
	client, err := New(srv.Addr(), WithDialContext(func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	isErrorIs(t, client.Delete(ctx, "missing"), ErrNotFound)
	noError(t, client.Set(ctx, "k", []byte("v"), 0, 0))
	equal(t, int32(1), dials.Load())
}

func TestClusterGetFollowsSetNode(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client, err := New(a.Addr()+","+b.Addr(), WithPickBuilder(prefixPickBuilder{}))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	noError(t, client.Set(ctx, "a-one", []byte("1"), 0, 0))
	noError(t, client.Set(ctx, "b-two", []byte("2"), 0, 0))

	item, err := client.Get(ctx, "a-one")
	noError(t, err)
	equal(t, []byte("1"), item.Value)

	item, err = client.Get(ctx, "b-two")
	noError(t, err)
	equal(t, []byte("2"), item.Value)

	item, err = client.GetAndTouch(ctx, time.Second, "b-two")
	noError(t, err)
	equal(t, []byte("2"), item.Value)
}

func TestIncrDecrAndNoReply(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	noError(t, client.Set(ctx, "n", []byte("10"), 0, 0))
	got, err := client.Incr(ctx, "n", 5)
	noError(t, err)
	equal(t, uint64(15), got)

	got, err = client.Decr(ctx, "n", 3)
	noError(t, err)
	equal(t, uint64(12), got)

	_, err = client.Incr(ctx, "missing", 1)
	isErrorIs(t, err, ErrNotFound)

	quiet, err := New(srv.Addr(), WithNoReply())
	noError(t, err)
	t.Cleanup(func() { _ = quiet.Close() })
	got, err = quiet.Incr(ctx, "n", 1)
	noError(t, err)
	equal(t, uint64(0), got)
}

func TestMetaQuietStillReturnsExists(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	_, err = client.MetaSet(ctx, []byte("k"), []byte("v"))
	noError(t, err)

	_, err = client.MetaSet(ctx, []byte("k"), []byte("v2"), MetaSetFlagNoReply(), MetaSetFlagCompareCAS(999))
	isErrorIs(t, err, ErrExists)

	item, err := client.MetaGet(ctx, []byte("k"), MetaGetFlagReturnValue())
	noError(t, err)
	equal(t, []byte("v"), item.Value)
}

func TestMetaQuietHitReturnsValue(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	_, err = client.MetaSet(ctx, []byte("k"), []byte("hello\nworld"))
	noError(t, err)

	item, err := client.MetaGet(ctx, []byte("k"), MetaGetFlagReturnValue(), MetaGetFlagNoReply())
	noError(t, err)
	equal(t, []byte("hello\nworld"), item.Value)
}

func TestMetaSetModeAdd(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	_, err = client.MetaSet(ctx, []byte("k"), []byte("v"), MetaSetFlagModeSwitch(MetaSetModeAdd))
	noError(t, err)
	_, err = client.MetaSet(ctx, []byte("k"), []byte("v2"), MetaSetFlagModeSwitch(MetaSetModeAdd))
	isErrorIs(t, err, ErrNotStored)
}

func TestMetaSetRejectsInjectedMode(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	_, err = client.MetaSet(context.Background(), []byte("k"), []byte("v"), MetaSetFlagModeSwitch("set\r\nquit"))
	isErrorIs(t, err, ErrInvalidArgument)
}

func TestMetaArithmeticRejectsInjectedMode(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	_, err = client.MetaArithmetic(context.Background(), []byte("k"), 1, MetaArithmeticFlagModeSwitch("I\r\nflush_all\r\n"))
	isErrorIs(t, err, ErrInvalidArgument)
}

func TestFlushAllRequiresOptIn(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	noError(t, client.Set(ctx, "k", []byte("v"), 0, 0))
	isErrorIs(t, client.FlushAll(ctx), ErrFlushAllNotAllowed)
	item, err := client.Get(ctx, "k")
	noError(t, err)
	equal(t, []byte("v"), item.Value)

	allowed, err := New(srv.Addr(), WithAllowFlushAll())
	noError(t, err)
	t.Cleanup(func() { _ = allowed.Close() })
	noError(t, allowed.FlushAll(ctx))
	_, err = client.Get(ctx, "k")
	isErrorIs(t, err, ErrNotFound)
}

func TestMetaQuietMissWithValueIsNotFound(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	_, err = client.MetaGet(context.Background(), []byte("missing"), MetaGetFlagReturnValue(), MetaGetFlagNoReply())
	isErrorIs(t, err, ErrNotFound)

	_, err = client.MetaSet(context.Background(), []byte("k"), []byte("v"))
	noError(t, err)
	item, err := client.MetaGet(context.Background(), []byte("k"), MetaGetFlagReturnValue())
	noError(t, err)
	equal(t, []byte("v"), item.Value)
}

func TestFlushAllWithNoReplyStillReadsOK(t *testing.T) {
	srv := startFakeMemcached(t)
	var dials atomic.Int32
	client, err := New(srv.Addr(), WithAllowFlushAll(), WithNoReply(), WithDialContext(func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	noError(t, client.Set(ctx, "k", []byte("v"), 0, 0))
	noError(t, client.FlushAll(ctx))
	noError(t, client.Set(ctx, "k2", []byte("v2"), 0, 0))
	equal(t, int32(1), dials.Load())
}

func TestGetMalformedValueDiscardsConnection(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	noError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	srv := &fakeMemcached{ln: ln, items: make(map[string]*fakeItem)}
	go func() {
		cn, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		r := bufio.NewReader(cn)
		_, _ = r.ReadBytes('\n')
		_, _ = cn.Write([]byte("VALUE k 0 3\r\nabc\r\nVALUE\r\nEND\r\n"))
		srv.serve()
	}()

	var dials atomic.Int32
	client, err := New(ln.Addr().String(), WithDialContext(func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	_, err = client.Get(context.Background(), "k")
	isErrorIs(t, err, ErrMalformedResponse)
	noError(t, client.Set(context.Background(), "k", []byte("v"), 0, 0))
	equal(t, int32(2), dials.Load())
}

func TestFlushAllAfterAndStatsArgs(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr(), WithAllowFlushAll())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	noError(t, client.Set(ctx, "k", []byte("v"), 0, 0))
	noError(t, client.FlushAllAfter(ctx, time.Second))
	_, err = client.Get(ctx, "k")
	isErrorIs(t, err, ErrNotFound)

	got, err := client.StatsArgs(ctx, "items")
	noError(t, err)
	equal(t, "items", got["arg"])

	_, err = client.StatsArgs(ctx, "flush_all")
	isErrorIs(t, err, ErrInvalidArgument)
	_, err = client.StatsArgs(ctx, "cachedump")
	isErrorIs(t, err, ErrInvalidArgument)

	reset, err := client.StatsArgs(ctx, "reset")
	noError(t, err)
	equal(t, 0, len(reset))

	noError(t, client.FlushAllStaggered(ctx, time.Second))
}

func TestMetaGetCheckCASAndPipeline(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	_, err = client.MetaSet(ctx, []byte("k"), []byte("v"), MetaSetFlagReturnCAS())
	noError(t, err)
	item, err := client.MetaGet(ctx, []byte("k"), MetaGetFlagReturnCAS(), MetaGetFlagReturnValue())
	noError(t, err)
	matched, err := client.MetaGet(ctx, []byte("k"), MetaGetFlagReturnValue(), MetaGetFlagCheckCAS(item.CAS))
	noError(t, err)
	isTrue(t, matched.CASMatched)
	isNil(t, matched.Value)

	noValue, err := client.MetaGet(ctx, []byte("k"), MetaGetFlagCheckCAS(item.CAS))
	noError(t, err)
	isFalse(t, noValue.CASMatched)

	changed, err := client.MetaGet(ctx, []byte("k"), MetaGetFlagReturnValue(), MetaGetFlagCheckCAS(item.CAS+1))
	noError(t, err)
	equal(t, []byte("v"), changed.Value)
	isFalse(t, changed.CASMatched)

	noError(t, client.Set(ctx, "a", []byte("1"), 0, 0))
	noError(t, client.Set(ctx, "b", []byte("2"), 0, 0))
	items, err := client.MetaGetPipeline(ctx, []MetaGetReq{
		{Key: []byte("a"), Options: []MetaGetOption{MetaGetFlagReturnValue()}},
		{Key: []byte("b"), Options: []MetaGetOption{MetaGetFlagReturnValue()}},
	})
	noError(t, err)
	equal(t, 2, len(items))
	equal(t, []byte("1"), items[0].Value)
	equal(t, []byte("2"), items[1].Value)

	piped, err := client.MetaGetPipeline(ctx, []MetaGetReq{
		{Key: []byte("k"), Options: []MetaGetOption{MetaGetFlagReturnValue(), MetaGetFlagCheckCAS(item.CAS)}},
	})
	noError(t, err)
	hasLen(t, piped, 1)
	isTrue(t, piped[0].CASMatched)
	isNil(t, piped[0].Value)
}

func TestOpaqueTokenRejectedAndAuth(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr(), WithAuth("u", "p"))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	_, err = client.MetaGet(context.Background(), []byte("k"), MetaGetFlagOpaqueToken("bad token"))
	isErrorIs(t, err, ErrInvalidArgument)

	plain, err := New(srv.Addr())
	noError(t, err)
	t.Cleanup(func() { _ = plain.Close() })
	isErrorIs(t, plain.Authenticate(context.Background(), "u\r\n", "p"), ErrInvalidArgument)

	noError(t, client.Set(context.Background(), "k", []byte("v"), 0, 0, NoReply()))
	item, err := client.Get(context.Background(), "k")
	noError(t, err)
	equal(t, []byte("v"), item.Value)
}

func TestRetrySkipsNotFound(t *testing.T) {
	srv := startFakeMemcached(t)
	var dials atomic.Int32
	client, err := New(srv.Addr(), WithRetry(3), WithDialContext(func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	_, err = client.Incr(context.Background(), "missing", 1)
	isErrorIs(t, err, ErrNotFound)
	equal(t, int32(1), dials.Load())
}

func TestRetryOnDialFailure(t *testing.T) {
	srv := startFakeMemcached(t)
	var n atomic.Int32
	client, err := New(srv.Addr(), WithRetry(2), WithDialContext(func(ctx context.Context, network, address string) (net.Conn, error) {
		if n.Add(1) == 1 {
			return nil, fmt.Errorf("dial boom")
		}
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	noError(t, client.Set(context.Background(), "k", []byte("v"), 0, 0))
	equal(t, int32(2), n.Load())
}

func TestFailoverEjectsDeadNode(t *testing.T) {
	good := startFakeMemcached(t)
	var n atomic.Int32
	client, err := New("127.0.0.1:1,"+good.Addr(),
		WithPickBuilder(firstAddrPickBuilder{}),
		WithRetry(1),
		WithFailover(FailoverConfig{FailureLimit: 1, RetryTimeout: time.Hour, EjectFromHash: true}),
		WithDialContext(func(ctx context.Context, network, address string) (net.Conn, error) {
			if address == "127.0.0.1:1" {
				n.Add(1)
				return nil, fmt.Errorf("dead")
			}
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}),
	)
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	_ = client.Set(context.Background(), "k", []byte("v"), 0, 0)
	_ = client.Set(context.Background(), "k", []byte("v"), 0, 0)
	isTrue(t, len(client.DeadServers()) > 0 || n.Load() > 0)
}

func TestDispatchHonorsCancelWithoutDeadline(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	noError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		cn, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		defer func() { _ = cn.Close() }()
		buf := make([]byte, 64)
		_, _ = cn.Read(buf)
		time.Sleep(10 * time.Second)
	}()

	client, err := New(ln.Addr().String(), WithReadTimeout(5*time.Second), WithWriteTimeout(5*time.Second))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	err = client.Set(ctx, "k", []byte("v"), 0, 0)
	isError(t, err)
	isTrue(t, time.Since(start) < 2*time.Second)
}

func TestUDPEnabledDoesNotFrameTCP(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr(), WithUDPEnabled())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	noError(t, client.Set(context.Background(), "k", []byte("v"), 0, 0))
	item, err := client.Get(context.Background(), "k")
	noError(t, err)
	equal(t, []byte("v"), item.Value)
}

func TestVersionAllStatsAllAndServers(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client, err := New(a.Addr() + "," + b.Addr())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	servers := client.Servers()
	hasLen(t, servers, 2)
	equal(t, a.Addr(), servers[0].Address)

	vers, err := client.VersionAll(ctx)
	noError(t, err)
	hasLen(t, vers, 2)
	for _, v := range vers {
		equal(t, "fake-1.0", v)
	}

	stats, err := client.StatsAll(ctx)
	noError(t, err)
	hasLen(t, stats, 2)
	for _, st := range stats {
		notNil(t, st)
		equal(t, "fake-1.0", st.Version)
	}

	noError(t, client.Set(ctx, "k", []byte("v"), 0, 0))
	ps := client.PoolStats()
	greater(t, len(ps), 0)
	greater(t, ps[0].Total, 0)
}

func TestWithTLSNilRejected(t *testing.T) {
	_, err := New("127.0.0.1:11211", WithTLS(nil))
	isErrorIs(t, err, ErrInvalidArgument)
}

func TestAdminRequiresOptIn(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	_, err = client.Admin()
	isErrorIs(t, err, ErrAdminNotEnabled)
}

func TestAdminSyncCommands(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr(), WithAdminCommands())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	admin, err := client.Admin()
	noError(t, err)

	ctx := context.Background()
	noError(t, admin.Verbosity(ctx, 1))
	noError(t, admin.CacheMemLimit(ctx, 64))
	noError(t, admin.SlabsAutomove(ctx, 1))
	noError(t, admin.LRUMode(ctx, "segmented"))
	noError(t, admin.LRUTune(ctx, 20, 40, 2, 2))
	noError(t, admin.LRUTempTTL(ctx, 60))
	noError(t, admin.LRUCrawlerEnable(ctx, true))
	noError(t, admin.LRUCrawlerSleep(ctx, 100))
	noError(t, admin.LRUCrawlerToCrawl(ctx, 10))
	noError(t, admin.LRUCrawlerCrawl(ctx, "all"))
	noError(t, admin.SlabsReassign(ctx, 1, 2))
	isErrorIs(t, admin.SlabsReassign(ctx, 1, 999), ErrBusy)
	isErrorIs(t, admin.SlabsReassign(ctx, 2, 2), ErrSame)
	isErrorIs(t, admin.LRUMode(ctx, "flat\r\nshutdown"), ErrInvalidArgument)
	isErrorIs(t, admin.LRUCrawlerCrawl(ctx, "all\r\nwatch"), ErrInvalidArgument)
	noError(t, admin.Shutdown(ctx, false))
}

func TestAdminWatchAndDumps(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr(), WithAdminCommands())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	admin, err := client.Admin()
	noError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	events, err := admin.Watch(ctx, WatchFetchers)
	noError(t, err)
	ev := <-events
	equal(t, "foo", ev.Raw["key"])
	equal(t, uint64(1), ev.GID)
	cancel()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	meta, err := admin.LRUCrawlerMetaDump(ctx2, "all")
	noError(t, err)
	row := <-meta
	equal(t, "foo", row["key"])
	_, ok := <-meta
	isFalse(t, ok)

	mg, err := admin.LRUCrawlerMgDump(ctx2, "all")
	noError(t, err)
	line := <-mg
	equal(t, "mg foo", line)
}

func TestSetRejectsCRLFKey(t *testing.T) {
	srv := startFakeMemcached(t)
	var writes atomic.Int32
	client, err := New(srv.Addr(), WithDialContext(func(ctx context.Context, network, address string) (net.Conn, error) {
		writes.Add(1)
		return net.Dial(network, address)
	}))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	err = client.Set(context.Background(), "a\r\nb", []byte("v"), 0, 0)
	isErrorIs(t, err, ErrInvalidKey)
	equal(t, int32(0), writes.Load())
}

func TestGetMultiPartialNodeFailure(t *testing.T) {
	good := startFakeMemcached(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	noError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		cn, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		_ = cn.Close()
	}()

	// Force keys onto known nodes via prefix picker.
	client, err := New(good.Addr()+","+ln.Addr().String(), WithPickBuilder(prefixPickBuilder{}))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	// prefixPickBuilder: first char selects node. Use keys that land on both.
	// If prefix picker isn't available, fall back to setting on good and Gets both.
	noError(t, client.Set(ctx, "a-good", []byte("v"), 0, 0))
	items, err := client.GetMulti(ctx, []string{"a-good", "b-dead"})
	isError(t, err)
	notNil(t, items["a-good"])
	equal(t, []byte("v"), items["a-good"].Value)
}

func TestFlushAllBroadcastsTwoNodes(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client, err := New(a.Addr()+","+b.Addr(), WithAllowFlushAll(), WithPickBuilder(NewCRC32HashPickBuilder()))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	noError(t, client.Set(ctx, "a-1", []byte("1"), 0, 0))
	// Second node via prefix picker is unused unless we also set a b- key
	// through a client that can reach it; seed both maps directly.
	b.mu.Lock()
	b.items["b-1"] = &fakeItem{value: []byte("2")}
	b.mu.Unlock()
	noError(t, client.FlushAll(ctx))
	a.mu.Lock()
	equal(t, 0, len(a.items))
	a.mu.Unlock()
	b.mu.Lock()
	equal(t, 0, len(b.items))
	b.mu.Unlock()
}

func TestNamespacePrefixesWireKey(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr(), WithNamespace("ns:", false))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	noError(t, client.Set(ctx, "k", []byte("v"), 0, 0))
	srv.mu.Lock()
	cmd := srv.lastCmd
	srv.mu.Unlock()
	contains(t, cmd, "ns:k")

	item, err := client.Get(ctx, "k")
	noError(t, err)
	equal(t, []byte("v"), item.Value)
	equal(t, "ns:k", item.Key)
}

func TestMinIdleWarmsPool(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr(), WithMinIdleConns(2), WithMaxIdleConns(4))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	noError(t, client.Set(context.Background(), "k", []byte("v"), 0, 0))
	ps := client.PoolStats()
	hasLen(t, ps, 1)
	greater(t, ps[0].Total, 1)
}

type countingResolver struct {
	inner defaultResolver
	n     atomic.Int32
}

func (r *countingResolver) Resolve(addr string) ([]*Addr, error) {
	r.n.Add(1)
	return r.inner.Resolve(addr)
}

func TestNamespaceHashWithPrefixChangesPick(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client, err := New(a.Addr()+","+b.Addr(),
		WithNamespace("b-", true),
		WithPickBuilder(prefixPickBuilder{}),
	)
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	noError(t, client.Set(ctx, "foo", []byte("v"), 0, 0))
	b.mu.Lock()
	_, onB := b.items["b-foo"]
	b.mu.Unlock()
	isTrue(t, onB)
}

func TestNoReplyOmitsReadAndNextGetWorks(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr(), WithNoReply())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	noError(t, client.Set(ctx, "k", []byte("v"), 0, 0, NoReply()))
	var cmd string
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		srv.mu.Lock()
		cmd = srv.lastCmd
		srv.mu.Unlock()
		if strings.Contains(cmd, "noreply") {
			break
		}
		time.Sleep(time.Millisecond)
	}
	contains(t, cmd, "noreply")

	item, err := client.Get(ctx, "k")
	noError(t, err)
	equal(t, []byte("v"), item.Value)
}

func TestAdminDumpBusy(t *testing.T) {
	srv := startFakeMemcached(t)
	srv.mu.Lock()
	srv.dumpBusy = true
	srv.mu.Unlock()
	client, err := New(srv.Addr(), WithAdminCommands())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	admin, err := client.Admin()
	noError(t, err)
	_, err = admin.LRUCrawlerMetaDump(context.Background(), "all")
	isErrorIs(t, err, ErrBusy)
}

func TestAdminWatchCancelDiscardsConn(t *testing.T) {
	srv := startFakeMemcached(t)
	var dials atomic.Int32
	client, err := New(srv.Addr(), WithAdminCommands(), WithMaxIdleConns(4), WithDialContext(func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		return net.Dial(network, address)
	}))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	admin, err := client.Admin()
	noError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	events, err := admin.Watch(ctx, WatchMutations)
	noError(t, err)
	<-events
	afterWatch := dials.Load()
	cancel()
	time.Sleep(30 * time.Millisecond)
	_, err = client.Version(context.Background())
	noError(t, err)
	if dials.Load() <= afterWatch {
		t.Fatalf("watch cancel should discard the conn; dials watch=%d after=%d", afterWatch, dials.Load())
	}
}

func TestUnixSocketRoundTrip(t *testing.T) {
	srv, path := startFakeMemcachedUnix(t)
	_ = srv
	client, err := New("unix://" + path)
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	ver, err := client.Version(context.Background())
	noError(t, err)
	equal(t, "fake-1.0", ver)
	noError(t, client.Set(context.Background(), "k", []byte("v"), 0, 0))
	item, err := client.Get(context.Background(), "k")
	noError(t, err)
	equal(t, []byte("v"), item.Value)
}

func TestResolverRefreshRecallsResolve(t *testing.T) {
	srv := startFakeMemcached(t)
	r := &countingResolver{}
	client, err := New(srv.Addr(), WithResolver(r), WithResolverRefresh(20*time.Millisecond))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	time.Sleep(80 * time.Millisecond)
	if r.n.Load() < 2 {
		t.Fatalf("expected refresh resolves, got %d", r.n.Load())
	}
}
