package memcached_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/EyJunge1/go-memcached/memcached"
)

func Example() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.Store(ctx, memcached.Item{Key: []byte("key"), Value: []byte("value")}); err != nil {
		fmt.Println("error:", err)
		return
	}
	var got memcached.GetResult
	err = client.GetOne(ctx, []byte("key"), &got, memcached.GetOpts{})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	_ = got
}

func ExampleNew() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()
}

func ExampleConfig() {
	cfg := memcached.Config{
		Servers:  []memcached.Server{{Address: "localhost:11211"}},
		Pool:     memcached.Pool{MaxOpen: 20},
		Failover: memcached.DefaultFailover(),
	}
	client, err := memcached.New(cfg)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()
}

func ExampleClient_Store() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	err = client.Store(context.Background(), memcached.Item{
		Key:   []byte("key"),
		Value: []byte("value"),
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
}

func ExampleClient_Get() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	dst := make([]memcached.GetResult, 1)
	err = client.Get(context.Background(), [][]byte{[]byte("key")}, dst, memcached.GetOpts{})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	_ = dst[0]
}

func ExampleClient_Close() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	if err := client.Close(); err != nil {
		fmt.Println("error:", err)
		return
	}
}

func ExampleRel() {
	_ = memcached.Item{Key: []byte("k"), Value: []byte("v"), Exp: memcached.Rel(30 * time.Second)}
}

func ExampleParseServers() {
	servers, err := memcached.ParseServers("localhost:11211,unix:///tmp/memcached.sock")
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	_ = servers
}

func ExampleConfig_failover() {
	_ = memcached.Config{
		Servers:  []memcached.Server{{Address: "localhost:11211"}},
		Failover: memcached.DefaultFailover(),
	}
}

func ExampleConfig_retryBackoff() {
	_ = memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
		Transport: memcached.Transport{
			RetryAttempts: 3,
			RetryBackoff:  25 * time.Millisecond, // same-node pause; failover hops skip wait
		},
		Failover: memcached.DefaultFailover(),
	}
}

func ExampleClient_Delete() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	if err := client.Delete(context.Background(), []byte("key"), memcached.MutateOpts{}); err != nil {
		fmt.Println("error:", err)
		return
	}
}

func ExampleClient_Incr() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	var n uint64
	if err := client.Incr(context.Background(), []byte("n"), 1, &n, memcached.ArithmeticOpts{}); err != nil {
		fmt.Println("error:", err)
		return
	}
	_ = n
}

func ExampleUnixExp() {
	exp, err := memcached.UnixExp(time.Now().Add(time.Hour))
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	_ = memcached.Item{Key: []byte("k"), Value: []byte("v"), Exp: exp}
}

func ExampleDefaultFailover() {
	_ = memcached.DefaultFailover()
}

func ExampleConfig_ApplyProductionDefaults() {
	cfg := memcached.Config{Servers: []memcached.Server{{Address: "localhost:11211"}}}
	cfg.ApplyProductionDefaults()
	client, err := memcached.New(cfg)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()
	_ = client.LiveServers()
}

func ExampleNewProductionFromAddrs() {
	client, err := memcached.NewProductionFromAddrs("localhost:11211")
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()
}

func ExampleMetaGetItem() {
	_ = memcached.MetaGetItem()
}

func ExampleClient_GetOne() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	var got memcached.GetResult
	err = client.GetOne(context.Background(), []byte("key"), &got, memcached.GetOpts{})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	if !got.Found {
		return
	}
	_ = got.Value
}

func ExampleClient_Get_found() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	dst := make([]memcached.GetResult, 1)
	err = client.Get(context.Background(), [][]byte{[]byte("key")}, dst, memcached.GetOpts{})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	if dst[0].Found {
		_ = dst[0].Value
	}
}

func ExampleClient_Touch() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	err = client.Touch(context.Background(), []byte("key"), memcached.Rel(time.Minute), memcached.MutateOpts{})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
}

func ExampleClient_Store_errorsIs() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	err = client.Store(context.Background(), memcached.Item{
		Mode:  memcached.StoreAdd,
		Key:   []byte("key"),
		Value: []byte("value"),
	})
	if errors.Is(err, memcached.ErrNotStored) {
		return
	}
	if err != nil {
		fmt.Println("error:", err)
		return
	}
}

func ExampleClient_Incr_afterStore() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	if err := client.Store(ctx, memcached.Item{Key: []byte("n"), Value: []byte("10")}); err != nil {
		fmt.Println("error:", err)
		return
	}
	var n uint64
	if err := client.Incr(ctx, []byte("n"), 1, &n, memcached.ArithmeticOpts{}); err != nil {
		fmt.Println("error:", err)
		return
	}
	_ = n
}
