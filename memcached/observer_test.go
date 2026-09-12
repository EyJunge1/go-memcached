package memcached

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"
)

type recordingObserver struct {
	mu     sync.Mutex
	events []OpEvent
}

var _ Observer = (*recordingObserver)(nil)

func (o *recordingObserver) ObserveOp(_ context.Context, e OpEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, e)
}

func (o *recordingObserver) snapshot() []OpEvent {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]OpEvent, len(o.events))
	copy(out, o.events)
	return out
}

func TestObserverNilIsZeroCost(t *testing.T) {
	ln := mustListen(t, "tcp", "127.0.0.1:0")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		acceptClose(ln)
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		wg.Wait()
	})

	client, err := New(Config{
		Servers: []Server{{Address: ln.Addr().String()}},
		Transport: Transport{
			DialTimeout:  time.Second,
			ReadTimeout:  time.Second,
			WriteTimeout: time.Second,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	if client.observer() != nil {
		t.Fatal("expected nil observer")
	}
}

func TestObserverSeesStoreAndGetOne(t *testing.T) {
	srv := startFakeMemcached(t)

	obs := &recordingObserver{}
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Observer = obs
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	mustStore(ctx, t, client, []byte("k"), []byte("v"))
	_ = mustGet(ctx, t, client, []byte("k"))

	events := obs.snapshot()
	if len(events) < 2 {
		t.Fatalf("events=%d want>=2 %#v", len(events), events)
	}
	foundSet, foundGet := false, false
	for _, e := range events {
		if e.Duration <= 0 {
			t.Fatalf("duration=%v", e.Duration)
		}
		if e.Keys < 1 {
			t.Fatalf("keys=%d", e.Keys)
		}
		switch e.Op {
		case "set":
			foundSet = true
		case "get":
			foundGet = true
		}
	}
	if !foundSet || !foundGet {
		t.Fatalf("ops missing set/get in %#v", events)
	}
}

func TestObserverSeesPipeline(t *testing.T) {
	srv := startFakeMemcached(t)

	obs := &recordingObserver{}
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Observer = obs
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := client.Pipeline().
		Store(Item{Key: []byte("a"), Value: []byte("1")}).
		Store(Item{Key: []byte("b"), Value: []byte("2")}).
		Run(ctx)
	if err != nil {
		t.Fatal(err)
	}

	var pipe OpEvent
	found := false
	for _, e := range obs.snapshot() {
		if e.Op == OpPipeline {
			pipe = e
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("no pipeline event in %#v", obs.snapshot())
	}
	if pipe.Keys != 2 {
		t.Fatalf("keys=%d want 2", pipe.Keys)
	}
	if pipe.Err != nil {
		t.Fatalf("err=%v", pipe.Err)
	}
}

func TestObserverSeesAdminAndStats(t *testing.T) {
	srv := startFakeMemcached(t)

	obs := &recordingObserver{}
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Observer = obs
		cfg.AllowAdmin = true
		cfg.AllowFlushAll = true
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var stats []Stat
	if err := client.Stats(ctx, &stats); err != nil {
		t.Fatal(err)
	}
	admin, err := client.Admin()
	if err != nil {
		t.Fatal(err)
	}
	if err := admin.On(client.Servers()[0]).Flush(ctx, FlushOpts{}); err != nil {
		t.Fatal(err)
	}

	snap := obs.snapshot()
	ops := make(map[string]bool, len(snap))
	for _, e := range snap {
		ops[e.Op] = true
	}
	if !ops["stats"] {
		t.Fatalf("missing stats in %#v", obs.snapshot())
	}
	if !ops["flush_all"] {
		t.Fatalf("missing flush_all in %#v", obs.snapshot())
	}
}

func acceptClose(ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		_ = c.Close()
	}
}
