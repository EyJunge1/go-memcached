package memcached

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func setupLiveClient(t *testing.T) *Client {
	t.Helper()
	cfg, err := testConfig("localhost:11211", func(cfg *Config) {
		cfg.Transport.DialTimeout = 200 * time.Millisecond
	})
	noError(t, err)
	c, err := New(cfg)
	if err != nil {
		t.Skip("no memcached on localhost:11211")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := c.VersionAll(ctx); err != nil {
		_ = c.Close()
		t.Skip("no memcached on localhost:11211")
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("close client: %v", err)
		}
	})
	return c
}

func Test_concurrent_dispatchRequest(t *testing.T) {
	client := setupLiveClient(t)
	key := "Test_concurrent_dispatchRequest"
	ctx := context.Background()

	err := client.Store(ctx, Item{Mode: StoreSet, Key: []byte(key), Value: []byte("Test_concurrent_dispatchRequest")})
	noError(t, err)

	wg := sync.WaitGroup{}
	limits := 100
	for range 10 {
		wg.Go(func() {
			for range limits {
				req, resp := buildGetsCommand("get", []byte(key))
				err := client.dispatchRequest(ctx, req, resp)
				noError(t, err)
			}
		})
	}

	wg.Wait()
}

// https://github.com/EyJunge1/go-memcached/issues/18
// Mock a concurrent client to set, get and touch the cache at the same time.
func Test_concurrent(t *testing.T) {
	client := setupLiveClient(t)
	// metaset options
	msOptions := func(cas uint64, clientFlags uint32) MetaSet {
		exp := Exptime(2)
		return MetaSet{
			NewCAS:      &cas,
			TTL:         &exp,
			ClientFlags: clientFlags,
			ReturnKey:   true,
			ReturnSize:  true,
			ReturnCAS:   true,
		}
	}

	key := "Test_concurrent"
	value := "Test_concurrent is value of Test_concurrent"
	// prepare data
	ctx := context.Background()

	var wg sync.WaitGroup

	// meta set goroutine
	wg.Go(func() {
		counter := 0
		cas := uint64(0)

		// update only
		for counter <= 20 {
			item, err := client.MetaSet(ctx, []byte(key), []byte(value), msOptions(cas, 0x1234))
			noError(t, err)
			if err != nil {
				return
			}

			cas = item.Item.CAS
			counter++

			time.Sleep(20 * time.Millisecond)
		}
	})

	// get goroutine
	wg.Go(func() {
		counter := 0

		for counter <= 200 {
			items, err := client.Get(ctx, GetOpts{}, []byte(key))
			noError(t, err)
			if len(items) == 0 {
				goto next
			}
			equal(t, value, string(items[0].Value))
			counter++

		next:
			time.Sleep(5 * time.Millisecond)
		}
	})

	// touch goroutine
	wg.Go(func() {
		counter := 0

		for counter <= 100 {
			err := client.Touch(ctx, []byte(key), 3, MutateOpts{})
			if !errors.Is(err, ErrNotFound) {
				noError(t, err)
			}

			counter++

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
		t.Fatal("Test_concurrent timed out (live memcached hung or never stored the key)")
	}

	t.Log("Test_concurrent finished")
}

func Test_classicReadCommandsRoundTrip(t *testing.T) {
	ctx := context.Background()
	client := setupLiveClient(t)

	value := []byte("hello hello hello hello hello hello")
	flag := uint32(0x1234)
	key1 := "Test_classicReadCommandsRoundTrip_1"
	key2 := "Test_classicReadCommandsRoundTrip_2"

	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte(key1), Value: value, Flags: flag}))
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte(key2), Value: value, Flags: flag}))

	assertItem := func(item *Item) {
		notNil(t, item)
		equal(t, value, item.Value)
		equal(t, flag, item.Flags)
	}

	item := mustGet(t, client, ctx, []byte(key1))
	assertItem(item)

	items, err := client.Get(ctx, GetOpts{CAS: true}, []byte(key1), []byte(key2))
	noError(t, err)
	hasLen(t, items, 2)
	for _, item := range items {
		assertItem(item)
		notZero(t, item.CAS)
	}

	exp := Rel(time.Second)
	items, err = client.Get(ctx, GetOpts{Touch: &exp}, []byte(key1))
	noError(t, err)
	hasLen(t, items, 1)
	assertItem(items[0])

	items, err = client.Get(ctx, GetOpts{CAS: true, Touch: &exp}, []byte(key1), []byte(key2))
	noError(t, err)
	hasLen(t, items, 2)
	for _, item := range items {
		assertItem(item)
		notZero(t, item.CAS)
	}
}

func Test_metaReadRoundTrip(t *testing.T) {
	ctx := context.Background()
	client := setupLiveClient(t)

	key := []byte("Test_metaReadRoundTrip")
	value := []byte("hello hello hello hello hello hello")
	flag := uint32(0x2345)

	stored, err := client.MetaSet(ctx, key, value, MetaSet{ClientFlags: flag})
	noError(t, err)
	equal(t, flag, stored.Item.Flags)

	item, err := client.MetaGet(ctx, key, MetaGet{Return: MetaGetReturn{Value: true, Flags: true}})
	noError(t, err)
	equal(t, value, item.Item.Value)
	equal(t, flag, item.Item.Flags)
}

func Test_udp(t *testing.T) {
	udpCfg, err := testConfig("udp://localhost:11211", func(cfg *Config) {
		cfg.Transport.DialTimeout = 200 * time.Millisecond
	})
	if err != nil {
		t.Skip("no UDP memcached on localhost:11211")
	}
	c, err := New(udpCfg)
	if err != nil {
		t.Skip("no UDP memcached on localhost:11211")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	ver, err := c.Version(ctx)
	_ = c.Close()
	if err != nil {
		t.Skip("no UDP memcached on localhost:11211")
	}
	notEmpty(t, ver)
}

func Test_unix(t *testing.T) {
	unixCfg, err := testConfig("unix:///tmp/memcached.sock", func(cfg *Config) {
		cfg.Transport.DialTimeout = 200 * time.Millisecond
	})
	if err != nil {
		t.Skip("no unix memcached at /tmp/memcached.sock")
	}
	c, err := New(unixCfg)
	if err != nil {
		t.Skip("no unix memcached at /tmp/memcached.sock")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := c.VersionAll(ctx); err != nil {
		_ = c.Close()
		t.Skip("no unix memcached at /tmp/memcached.sock")
	}
	t.Cleanup(func() { _ = c.Close() })
	ver, err := c.Version(ctx)
	noError(t, err)
	notEmpty(t, ver)
}
