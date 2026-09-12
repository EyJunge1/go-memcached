package memcached

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
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
	ln    net.Listener
	mu    sync.Mutex
	items map[string]*fakeItem
	cas   uint64
}

func startFakeMemcached(t *testing.T) *fakeMemcached {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
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
		case "set", "add", "replace", "append", "prepend":
			if !f.store(fields, r, w) {
				return
			}
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
	body := make([]byte, nbytes+2)
	if _, err = io.ReadFull(r, body); err != nil {
		return false
	}
	value := body[:nbytes]

	f.mu.Lock()
	f.cas++
	f.items[key] = &fakeItem{value: append([]byte(nil), value...), flags: uint32(flags64), cas: f.cas}
	f.mu.Unlock()

	_, _ = w.WriteString("STORED\r\n")
	_ = w.Flush()
	return true
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
