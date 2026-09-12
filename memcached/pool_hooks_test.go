package memcached

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPoolHooksOnAcquireAndOnCreate(t *testing.T) {
	ln := mustListen(t, "tcp", "127.0.0.1:0")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		wg.Wait()
	})

	var acquires, creates atomic.Int32
	client, err := New(Config{
		Servers: []Server{{Address: ln.Addr().String()}},
		Pool: Pool{
			MaxOpen: 2,
			MaxIdle: 1,
			OnAcquire: func(ev PoolEvent) {
				acquires.Add(1)
				if ev.Server.Address != ln.Addr().String() {
					t.Errorf("acquire server=%q", ev.Server.Address)
				}
			},
			OnCreate: func(ev PoolEvent) {
				creates.Add(1)
				if ev.Err != nil {
					t.Errorf("create err=%v", ev.Err)
				}
			},
		},
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

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cn, err := client.getConn(ctx, client.addrs[0])
	if err != nil {
		t.Fatal(err)
	}
	_ = cn.release()

	// First checkout dials: OnCreate fires; OnAcquire only on MaxOpen waits.
	if creates.Load() < 1 {
		t.Fatalf("creates=%d", creates.Load())
	}
	if acquires.Load() != 0 {
		t.Fatalf("acquires=%d want 0 without MaxOpen wait", acquires.Load())
	}

	// Saturate MaxOpen and force a wait.
	c1, err := client.getConn(ctx, client.addrs[0])
	if err != nil {
		t.Fatal(err)
	}
	c2, err := client.getConn(ctx, client.addrs[0])
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		c3, err := client.getConn(ctx, client.addrs[0])
		if err == nil {
			_ = c3.release()
		}
	}()
	time.Sleep(20 * time.Millisecond)
	_ = c1.release()
	_ = c2.release()
	<-done
	if acquires.Load() < 1 {
		t.Fatalf("acquires=%d after MaxOpen wait", acquires.Load())
	}
}

func TestMaxPipelineOpsValidation(t *testing.T) {
	_, err := New(Config{
		Servers: []Server{{Address: "127.0.0.1:11211"}},
		Cluster: Cluster{MaxPipelineOps: -1},
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDefaultPickerIsRendezvousXXH3(t *testing.T) {
	c, err := New(Config{
		Servers: []Server{
			{Address: "10.0.0.1:11211"},
			{Address: "10.0.0.2:11211"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if _, ok := c.picker.(*rendezvousPicker); !ok {
		t.Fatalf("picker=%T", c.picker)
	}
}
