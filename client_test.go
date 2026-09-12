package memcached

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func setupLiveClient(t *testing.T) *client {
	t.Helper()
	c, err := newClientWithContext(context.Background(), "localhost:11211")
	if err != nil {
		t.Fatal(err)
	}
	cc := c.(*client)
	t.Cleanup(func() {
		if err := cc.Close(); err != nil {
			t.Errorf("close client: %v", err)
		}
	})
	return cc
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

// https://github.com/EyJunge1/gomemcached/issues/18
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

	wg := sync.WaitGroup{}
	wg.Add(3)

	// meta set goroutine
	go func() {
		defer wg.Done()

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
	}()

	// get goroutine
	go func() {
		defer wg.Done()

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
	}()

	// touch goroutine
	go func() {
		defer wg.Done()
		counter := 0

		for counter <= 100 {
			err := client.Touch(ctx, key, 3)
			if !errors.Is(err, ErrNotFound) {
				noError(t, err)
			}

			counter++

			time.Sleep(10 * time.Millisecond)
		}
	}()

	wg.Wait()

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
	t.Skipf("skip test_udp, since it could not run in CI")

	addrs := "udp://localhost:11211"
	c, err := newClientWithContext(context.Background(), addrs, WithUDPEnabled())
	if err != nil {
		t.Fatalf("Failed to create client: %+v", err)
	}
	noError(t, err)
	notNil(t, c)

	ver, err := c.Version(context.Background())
	if err != nil {
		t.Fatalf("Failed to get version: %+v", err)
	}
	noError(t, err)
	notEmpty(t, ver)

	t.Logf("version: %s", ver)
}

func Test_unix(t *testing.T) {
	t.Skipf("skip test_unix, since it could not run in CI")

	addrs := "unix:///tmp/memcached.sock"
	c, err := newClientWithContext(context.Background(), addrs)
	noError(t, err)
	notNil(t, c)

	ver, err := c.Version(context.Background())
	noError(t, err)
	notEmpty(t, ver)

	t.Logf("version: %s", ver)
}
