package memcached

import (
	"context"
	"io"
	"sync"
	"testing"
)

func TestSharedStreamConcurrentStoreGet(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Pool.SharedStreams = 2
		cfg.Pool.MaxInFlightWrites = 64
		cfg.Pool.MaxOpen = 4
	})
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := []byte{"abcdefghijklmnopqrstuvwxyz"[i%26], "01"[i/26]}
			noError(t, c.Store(ctx, Item{Key: key, Value: []byte("v")}))
			var got GetResult
			noError(t, c.GetOne(ctx, key, &got, GetOpts{}))
			isTrue(t, got.Found)
		}(i)
	}
	wg.Wait()
}

func TestSharedStreamPipelineWave(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Pool.SharedStreams = 1
		cfg.Pool.MaxInFlightWrites = 32
	})
	ctx := context.Background()

	p := c.Pipeline()
	var results [8]PipelineResult
	for i := range results {
		key := []byte{byte('p'), byte('0' + i)}
		p.Store(Item{Key: key, Value: []byte("x")}, &results[i])
	}
	noError(t, p.Run(ctx))
	for i := range results {
		noError(t, results[i].Err)
		equal(t, PipelineDone, results[i].State)
	}

	var got GetResult
	noError(t, c.GetOne(ctx, []byte("p0"), &got, GetOpts{}))
	isTrue(t, got.Found)
}

func TestReadFullFast(t *testing.T) {
	r := &shortReader{data: []byte("abcdef"), chunk: 2}
	buf := make([]byte, 6)
	noError(t, readFullFast(r, buf))
	equal(t, "abcdef", string(buf))

	r2 := &shortReader{data: []byte("xy"), chunk: 2}
	buf2 := make([]byte, 2)
	noError(t, readFullFast(r2, buf2))
	equal(t, "xy", string(buf2))
}

type shortReader struct {
	data  []byte
	chunk int
	off   int
}

func (s *shortReader) Read(p []byte) (int, error) {
	if s.off >= len(s.data) {
		return 0, io.EOF
	}
	n := s.chunk
	if n > len(p) {
		n = len(p)
	}
	if s.off+n > len(s.data) {
		n = len(s.data) - s.off
	}
	copy(p, s.data[s.off:s.off+n])
	s.off += n
	return n, nil
}
