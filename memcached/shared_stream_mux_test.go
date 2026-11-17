package memcached

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSharedStreamMetaConcurrentGet(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Pool.SharedStreams = 1
		cfg.Pool.MaxInFlightWrites = 64
	})
	ctx := context.Background()
	meta := c.Meta()

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Appendf(nil, "a%d", i)
			val := fmt.Appendf(nil, "A%d", i)
			var setDst MetaResult
			noError(t, meta.Set(ctx, key, val, &setDst, MetaSet{}))
			var got MetaResult
			noError(t, meta.Get(ctx, key, &got, MetaGetValue()))
			equal(t, MetaStatusVA, got.Status)
			equal(t, val, got.Item.Value)
		}(i)
	}
	wg.Wait()
}

func TestSharedStreamMetaQuietBatch(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Pool.SharedStreams = 1
		cfg.Pool.MaxInFlightWrites = 32
	})
	ctx := context.Background()
	meta := c.Meta()

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Appendf(nil, "q%d", i)
			var dst MetaResult
			noError(t, meta.Set(ctx, key, []byte("v"), &dst, MetaSet{Quiet: true}))
		}(i)
	}
	wg.Wait()

	for i := 0; i < 16; i++ {
		key := fmt.Appendf(nil, "q%d", i)
		var got MetaResult
		noError(t, meta.Get(ctx, key, &got, MetaGetValue()))
		isTrue(t, got.Status == MetaStatusVA)
		equal(t, []byte("v"), got.Item.Value)
	}
}

func TestSharedStreamMetaPipelineAndSingle(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Pool.SharedStreams = 1
		cfg.Pool.MaxInFlightWrites = 64
	})
	ctx := context.Background()
	meta := c.Meta()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ops := make([]MetaOp, 8)
		dst := make([]MetaResult, 8)
		for i := range ops {
			ops[i] = MetaOp{
				Key:  []byte{byte('p'), byte('0' + i)},
				Body: []byte("x"),
				Set:  &MetaSet{},
			}
		}
		noError(t, meta.Pipeline(ctx, ops, dst))
		for i := range dst {
			noError(t, dst[i].Err())
		}
	}()

	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Appendf(nil, "s%d", i)
			var dst MetaResult
			noError(t, meta.Set(ctx, key, []byte("y"), &dst, MetaSet{}))
			var got MetaResult
			noError(t, meta.Get(ctx, key, &got, MetaGetValue()))
			equal(t, []byte("y"), got.Item.Value)
		}(i)
	}
	wg.Wait()
}

func TestSharedStreamTextWaitsForMeta(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Pool.SharedStreams = 1
		cfg.Pool.MaxInFlightWrites = 32
	})
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			key := []byte{byte('m'), byte('0' + i%10)}
			var dst MetaResult
			noError(t, c.Meta().Set(ctx, key, []byte("m"), &dst, MetaSet{}))
		}(i)
		go func(i int) {
			defer wg.Done()
			key := []byte{byte('t'), byte('0' + i%10)}
			noError(t, c.Store(ctx, Item{Key: key, Value: []byte("t")}))
			var got GetResult
			noError(t, c.GetOne(ctx, key, &got, GetOpts{}))
			isTrue(t, got.Found)
		}(i)
	}
	wg.Wait()
}

func TestSharedStreamMetaOpaqueReorder(t *testing.T) {
	ln := mustListen(t, "tcp", "127.0.0.1:0")
	t.Cleanup(func() { _ = ln.Close() })
	go serveOpaqueReorder(t, ln)

	c := testClient(t, ln.Addr().String(), func(cfg *Config) {
		cfg.Pool.SharedStreams = 1
		cfg.Pool.MaxInFlightWrites = 32
	})
	ctx := context.Background()
	meta := c.Meta()

	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := []byte("k" + strconv.Itoa(i))
			opaque := []byte(strconv.Itoa(i))
			var dst MetaResult
			errs[i] = meta.Get(ctx, key, &dst, MetaGetValue().WithOpaque(opaque))
			if dst.Status == MetaStatusVA {
				equal(t, []byte("v"+strconv.Itoa(i)), dst.Item.Value)
			}
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		noError(t, err)
	}
}

func serveOpaqueReorder(t testing.TB, ln net.Listener) {
	t.Helper()
	for {
		cn, err := ln.Accept()
		if err != nil {
			return
		}
		go func(cn net.Conn) {
			defer func() { _ = cn.Close() }()
			r := bufio.NewReader(cn)
			w := bufio.NewWriter(cn)
			var pending []string
			flushPending := func() {
				for i := len(pending) - 1; i >= 0; i-- {
					_, _ = w.WriteString(pending[i])
				}
				pending = pending[:0]
				_ = w.Flush()
			}
			for {
				line, err := r.ReadString('\n')
				if err != nil {
					return
				}
				fields := strings.Fields(strings.TrimSpace(line))
				if len(fields) == 0 {
					continue
				}
				switch fields[0] {
				case "mn":
					flushPending()
					_, _ = w.WriteString("MN\r\n")
					_ = w.Flush()
				case "mg":
					opaque := ""
					for _, tok := range fields[2:] {
						if strings.HasPrefix(tok, "O") {
							opaque = tok[1:]
						}
					}
					key := fields[1]
					val := "v" + strings.TrimPrefix(key, "k")
					reply := "VA " + strconv.Itoa(len(val))
					if opaque != "" {
						reply += " O" + opaque
					}
					reply += "\r\n" + val + "\r\n"
					pending = append(pending, reply)
					// Flush reversed when the buffer is large enough or next cmd forces it.
					if len(pending) >= 4 {
						flushPending()
					}
				default:
					flushPending()
				}
			}
		}(cn)
	}
}

func TestScanMetaRequestOpaqueAndInject(t *testing.T) {
	num, hasNum, tok, found := scanMetaRequestOpaque([]byte("mg key v h\r\n"))
	isTrue(t, !found)
	_ = num
	_ = hasNum
	_ = tok

	num, hasNum, _, found = scanMetaRequestOpaque([]byte("mg key v O7\r\n"))
	isTrue(t, found && hasNum)
	equal(t, uint64(7), num)

	raw, err := injectMetaRequestOpaque([]byte("mg key v\r\n"), "9")
	noError(t, err)
	equal(t, []byte("mg key v O9\r\n"), raw)

	box := make([]byte, 0, 32)
	box = append(box, "mg key v\r\n"...)
	req := &request{raw: box, rawBox: &box}
	noError(t, injectMetaRequestOpaqueUint(req, 9))
	equal(t, []byte("mg key v O9\r\n"), req.raw)
	isTrue(t, req.rawBox != nil)
	equal(t, []byte("mg key v O9\r\n"), *req.rawBox)

	j := &streamJob{
		req:  &request{raw: []byte("mg k v q\r\nmn\r\n"), cmd: _MgBytes},
		resp: buildQuietBarrierResponse(),
	}
	stripJobQuietNoop(j)
	equal(t, []byte("mg k v q\r\n"), j.req.raw)
}

func TestSharedStreamMetaUnknownOpaque(t *testing.T) {
	ln := mustListen(t, "tcp", "127.0.0.1:0")
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		cn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = cn.Close() }()
		r := bufio.NewReader(cn)
		w := bufio.NewWriter(cn)
		_, _ = r.ReadString('\n')
		_, _ = w.WriteString("HD O999\r\n")
		_ = w.Flush()
		_, _ = io.Copy(io.Discard, r)
	}()

	c := testClient(t, ln.Addr().String(), func(cfg *Config) {
		cfg.Pool.SharedStreams = 1
	})
	var dst MetaResult
	err := c.Meta().Get(context.Background(), []byte("k"), &dst, MetaGet{Opaque: []byte("1")})
	isError(t, err)
}

func TestSharedStreamMetaQuietAndLoudSameWindow(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Pool.SharedStreams = 1
		cfg.Pool.MaxInFlightWrites = 64
	})
	ctx := context.Background()
	meta := c.Meta()

	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			key := []byte{byte('L'), byte('0' + i%10)}
			var dst MetaResult
			noError(t, meta.Set(ctx, key, []byte("loud"), &dst, MetaSet{}))
		}(i)
		go func(i int) {
			defer wg.Done()
			key := []byte{byte('Q'), byte('0' + i%10)}
			var dst MetaResult
			noError(t, meta.Set(ctx, key, []byte("quiet"), &dst, MetaSet{Quiet: true}))
		}(i)
	}
	wg.Wait()

	for i := 0; i < 12; i++ {
		var got MetaResult
		noError(t, meta.Get(ctx, []byte{byte('L'), byte('0' + i%10)}, &got, MetaGetValue()))
		equal(t, []byte("loud"), got.Item.Value)
		noError(t, meta.Get(ctx, []byte{byte('Q'), byte('0' + i%10)}, &got, MetaGetValue()))
		equal(t, []byte("quiet"), got.Item.Value)
	}
}

func TestSharedStreamMetaWriteWhileReadOverlap(t *testing.T) {
	ln := mustListen(t, "tcp", "127.0.0.1:0")
	t.Cleanup(func() { _ = ln.Close() })

	var mu sync.Mutex
	maxBatched := 0

	go func() {
		for {
			cn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(cn net.Conn) {
				defer func() { _ = cn.Close() }()
				r := bufio.NewReader(cn)
				w := bufio.NewWriter(cn)
				for {
					line, err := r.ReadString('\n')
					if err != nil {
						return
					}
					fields := strings.Fields(strings.TrimSpace(line))
					if len(fields) == 0 || fields[0] != "mg" {
						continue
					}
					pending := []string{line}
					_ = cn.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
					for {
						more, err := r.ReadString('\n')
						if err != nil {
							break
						}
						pending = append(pending, more)
					}
					_ = cn.SetReadDeadline(time.Time{})
					mu.Lock()
					if len(pending) > maxBatched {
						maxBatched = len(pending)
					}
					mu.Unlock()
					for _, req := range pending {
						f := strings.Fields(strings.TrimSpace(req))
						if len(f) == 0 || f[0] != "mg" {
							if len(f) > 0 && f[0] == "mn" {
								_, _ = w.WriteString("MN\r\n")
							}
							continue
						}
						opaque := ""
						for _, tok := range f[2:] {
							if strings.HasPrefix(tok, "O") {
								opaque = tok[1:]
							}
						}
						reply := "HD"
						if opaque != "" {
							reply += " O" + opaque
						}
						_, _ = w.WriteString(reply + "\r\n")
					}
					_ = w.Flush()
				}
			}(cn)
		}
	}()

	c := testClient(t, ln.Addr().String(), func(cfg *Config) {
		cfg.Pool.SharedStreams = 1
		cfg.Pool.MaxInFlightWrites = 32
	})
	ctx := context.Background()
	meta := c.Meta()

	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var dst MetaResult
			errs[i] = meta.Get(ctx, []byte("k"+strconv.Itoa(i)), &dst, MetaGet{})
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		noError(t, err)
	}
	mu.Lock()
	got := maxBatched
	mu.Unlock()
	if got < 2 {
		t.Fatalf("expected coalesced/overlapped meta writes (>=2), got %d", got)
	}
}

func TestSharedStreamMetaCancelQueued(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Pool.SharedStreams = 1
		cfg.Pool.MaxInFlightWrites = 2
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var dst MetaResult
	err := c.Meta().Get(ctx, []byte("k"), &dst, MetaGetValue())
	isTrue(t, err != nil)
}

func TestSharedStreamCloseFailsInFlight(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Pool.SharedStreams = 1
	})
	ctx := context.Background()
	var dst MetaResult
	noError(t, c.Meta().Set(ctx, []byte("k"), []byte("v"), &dst, MetaSet{}))
	noError(t, c.Close())
	err := c.Meta().Get(ctx, []byte("k"), &dst, MetaGetValue())
	isTrue(t, err != nil)
}

func TestSharedStreamCompleteMuxFaultWithOpaque(t *testing.T) {
	addr, _ := startCmdServer(t, func(fields []string) string {
		if fields[0] == "mg" {
			opaque := "1"
			for _, tok := range fields[2:] {
				if len(tok) > 1 && tok[0] == 'O' {
					opaque = tok[1:]
				}
			}
			return "CLIENT_ERROR bad meta O" + opaque + "\r\n"
		}
		return "ERROR\r\n"
	})
	c := testClient(t, addr, func(cfg *Config) {
		cfg.Pool.SharedStreams = 1
	})
	var dst MetaResult
	err := c.Meta().Get(context.Background(), []byte("k"), &dst, MetaGetValue().WithOpaque([]byte("7")))
	isErrorIs(t, err, ErrClientError)
}

func TestSharedStreamQuietDefaultOnMN(t *testing.T) {
	ln := mustListen(t, "tcp", "127.0.0.1:0")
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			cn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(cn net.Conn) {
				defer func() { _ = cn.Close() }()
				r := bufio.NewReader(cn)
				w := bufio.NewWriter(cn)
				for {
					line, err := r.ReadString('\n')
					if err != nil {
						return
					}
					fields := strings.Fields(strings.TrimSpace(line))
					if len(fields) == 0 {
						continue
					}
					switch fields[0] {
					case "ms":
						nbytes := 0
						if len(fields) >= 3 {
							nbytes, _ = strconv.Atoi(fields[2])
						}
						if nbytes >= 0 {
							_, _ = io.ReadFull(r, make([]byte, nbytes+2))
						}
						// Quiet: no HD reply.
					case "mn":
						_, _ = w.WriteString("MN\r\n")
						_ = w.Flush()
					default:
						_, _ = w.WriteString("ERROR\r\n")
						_ = w.Flush()
					}
				}
			}(cn)
		}
	}()

	c := testClient(t, ln.Addr().String(), func(cfg *Config) {
		cfg.Pool.SharedStreams = 1
	})
	dsts := make([]MetaResult, 2)
	noError(t, c.Meta().Pipeline(context.Background(), []MetaOp{
		{Key: []byte("k2"), Body: []byte("v"), Set: &MetaSet{Quiet: true}},
		{Noop: &MetaNoop{}},
	}, dsts))
	equal(t, MetaStatusHD, dsts[0].Status)
	equal(t, MetaStatusMN, dsts[1].Status)
}

func TestSharedStreamFailWriteBatch(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Pool.SharedStreams = 1
		cfg.Pool.MaxInFlightWrites = 32
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			cn, err := (&net.Dialer{}).DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			return &failPipelineWriteConn{Conn: cn}, nil
		}
	})
	var dst MetaResult
	err := c.Meta().Set(context.Background(), []byte("k"), []byte("v"), &dst, MetaSet{})
	isErrorIs(t, err, ErrTransport)
}
