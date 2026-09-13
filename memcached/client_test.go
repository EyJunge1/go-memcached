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
	c, err := New("localhost:11211", WithDialTimeout(200*time.Millisecond))
	if err != nil {
		t.Skip("no memcached on localhost:11211")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := c.Ping(ctx); err != nil {
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

	err := client.Set(ctx, key, []byte("Test_concurrent_dispatchRequest"), 0, 0)
	noError(t, err)

	wg := sync.WaitGroup{}
	limits := 100
	for range 10 {
		wg.Go(func() {
			for range limits {
				req, resp := buildGetsCommand("get", key)
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
	msOptions := func(
		cas uint64, clientFlags uint32,
	) []MetaSetOption {
		expiration := 2
		return []MetaSetOption{
			MetaSetFlagNewCAS(cas),
			MetaSetFlagTTL(uint64(expiration)),
			MetaSetFlagClientFlags(clientFlags),
			MetaSetFlagReturnKey(),
			MetaSetFlagReturnSize(),
			MetaSetFlagReturnCAS(),
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
			item, err := client.MetaSet(ctx, []byte(key), []byte(value), msOptions(cas, 0x1234)...)
			noError(t, err)
			if err != nil {
				return
			}

			cas = item.CAS
			counter++

			time.Sleep(20 * time.Millisecond)
		}
	})

	// get goroutine
	wg.Go(func() {
		counter := 0

		for counter <= 200 {
			item, err := client.Get(ctx, key)
			if errors.Is(err, ErrNotFound) {
				goto next
			}
			noError(t, err)
			if err != nil {
				return
			}

			equal(t, value, string(item.Value))
			counter++

		next:
			time.Sleep(5 * time.Millisecond)
		}
	})

	// touch goroutine
	wg.Go(func() {
		counter := 0

		for counter <= 100 {
			err := client.Touch(ctx, key, 3)
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

	noError(t, client.Set(ctx, key1, value, flag, 0))
	noError(t, client.Set(ctx, key2, value, flag, 0))

	assertItem := func(item *Item) {
		notNil(t, item)
		equal(t, value, item.Value)
		equal(t, flag, item.Flags)
	}

	item, err := client.Get(ctx, key1)
	noError(t, err)
	assertItem(item)

	items, err := client.Gets(ctx, key1, key2)
	noError(t, err)
	hasLen(t, items, 2)
	for _, item := range items {
		assertItem(item)
		notZero(t, item.CAS)
	}

	item, err = client.GetAndTouch(ctx, time.Second, key1)
	noError(t, err)
	assertItem(item)

	items, err = client.GetAndTouches(ctx, time.Second, key1, key2)
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

	stored, err := client.MetaSet(ctx, key, value, MetaSetFlagClientFlags(flag))
	noError(t, err)
	equal(t, flag, stored.Flags)

	item, err := client.MetaGet(ctx, key, MetaGetFlagReturnValue(), MetaGetFlagReturnClientFlags())
	noError(t, err)
	equal(t, value, item.Value)
	equal(t, flag, item.Flags)
}

func Test_udp(t *testing.T) {
	c, err := New("udp://localhost:11211", WithDialTimeout(200*time.Millisecond))
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
	c, err := New("unix:///tmp/memcached.sock", WithDialTimeout(200*time.Millisecond))
	if err != nil {
		t.Skip("no unix memcached at /tmp/memcached.sock")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := c.Ping(ctx); err != nil {
		_ = c.Close()
		t.Skip("no unix memcached at /tmp/memcached.sock")
	}
	t.Cleanup(func() { _ = c.Close() })
	ver, err := c.Version(ctx)
	noError(t, err)
	notEmpty(t, ver)
}
