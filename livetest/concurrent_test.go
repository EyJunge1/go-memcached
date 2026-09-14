package livetest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/EyJunge1/go-memcached/memcached"
)

func TestConcurrentGet(t *testing.T) {
	client := setupLiveClient(t)
	key := []byte("TestConcurrentGet")
	ctx := context.Background()

	noError(t, client.Store(ctx, memcached.Item{
		Mode:  memcached.StoreSet,
		Key:   key,
		Value: []byte("TestConcurrentGet"),
	}))

	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			for range 100 {
				items, err := doGet(client, ctx, [][]byte{key}, memcached.GetOpts{})
				if err != nil {
					t.Errorf("get: %v", err)
					return
				}
				if len(items) != 1 || !items[0].Found {
					t.Errorf("get miss for %q", key)
					return
				}
			}
		})
	}
	wg.Wait()
}

func TestConcurrent(t *testing.T) {
	client := setupLiveClient(t)
	msOptions := func(cas uint64, clientFlags uint32) memcached.MetaSet {
		exp := memcached.Exptime(2)
		return memcached.MetaSet{
			NewCAS:     &cas,
			TTL:        &exp,
			Flags:      clientFlags,
			ReturnKey:  true,
			ReturnSize: true,
			ReturnCAS:  true,
		}
	}

	key := []byte("TestConcurrent")
	value := "Test_concurrent is value of Test_concurrent"
	ctx := context.Background()

	var wg sync.WaitGroup

	wg.Go(func() {
		counter := 0
		cas := uint64(0)

		for counter <= 20 {
			item, err := doMetaSet(client, ctx, key, []byte(value), msOptions(cas, 0x1234))
			if err != nil {
				t.Errorf("meta set: %v", err)
				return
			}
			cas = item.Item.CAS
			counter++
			time.Sleep(20 * time.Millisecond)
		}
	})

	wg.Go(func() {
		counter := 0
		for counter <= 200 {
			items, err := doGet(client, ctx, [][]byte{key}, memcached.GetOpts{})
			if err != nil {
				t.Errorf("get: %v", err)
				return
			}
			if len(items) > 0 {
				if string(items[0].Value) != value {
					t.Errorf("want %q, got %q", value, items[0].Value)
					return
				}
				counter++
			}
			time.Sleep(5 * time.Millisecond)
		}
	})

	wg.Go(func() {
		for range 101 {
			err := client.Touch(ctx, key, 3, memcached.MutateOpts{})
			if err != nil && !errors.Is(err, memcached.ErrNotFound) {
				t.Errorf("touch: %v", err)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	})

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(8 * time.Second):
		t.Fatal("TestConcurrent timed out (live memcached hung or never stored the key)")
	}
}
