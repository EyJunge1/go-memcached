package memcached_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/EyJunge1/go-memcached/memcached"
)

func ExampleMetaClient_Get() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	var res memcached.MetaResult
	err = client.Meta().Get(context.Background(), []byte("key"), &res, memcached.MetaGetValue())
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	if err := res.Err(); err != nil {
		log.Print(err)
		return
	}
	_ = res.Item.Value
}

func ExampleMetaClient_Get_recache() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	vivify := memcached.Rel(30 * time.Second)
	var res memcached.MetaResult
	err = client.Meta().Get(ctx, []byte("key"), &res, memcached.MetaGet{
		Vivify:  &vivify,
		Recache: 10 * time.Second,
		Return:  memcached.MetaGetReturn{Value: true},
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	if res.Item.Won {
		_ = client.Meta().Set(ctx, []byte("key"), []byte("fresh"), &res, memcached.MetaSet{})
	}
}

func ExampleMetaClient_GetOrFill() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	var res memcached.MetaResult
	err = client.Meta().GetOrFill(ctx, []byte("key"), &res, memcached.GetOrFillOpts{
		Vivify:  memcached.Rel(30 * time.Second),
		Recache: 10 * time.Second,
	}, func(_ context.Context, _ []byte) ([]byte, uint32, memcached.Exptime, error) {
		return []byte("fresh"), 0, memcached.Rel(time.Minute), nil
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	if err := res.Err(); err != nil {
		log.Print(err)
		return
	}
	_ = res.Item.Value
}

func ExampleMetaClient_Set() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	var res memcached.MetaResult
	err = client.Meta().Set(context.Background(), []byte("key"), []byte("value"), &res, memcached.MetaSet{
		Return: memcached.MetaSetReturn{CAS: true},
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	if err := res.Err(); err != nil {
		log.Print(err)
	}
}

func ExampleMetaClient_Delete() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	var res memcached.MetaResult
	err = client.Meta().Delete(context.Background(), []byte("key"), &res, memcached.MetaDelete{})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	if err := res.Err(); err != nil {
		log.Print(err)
	}
}

func ExampleMetaClient_Arithmetic() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	var res memcached.MetaResult
	ttl := memcached.Rel(time.Hour)
	initial := uint64(0)
	err = client.Meta().Arithmetic(context.Background(), []byte("n"), 1, &res, memcached.MetaArithmetic{
		AutoCreate: &ttl,
		Initial:    &initial,
		Return:     memcached.MetaArithmeticReturn{Value: true},
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	_ = res.Item.Value
}

func ExampleMetaClient_Debug() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	var dst memcached.MetaItemDebug
	err = client.Meta().Debug(context.Background(), []byte("key"), &dst, memcached.MetaDebug{})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	if err := dst.Err(); err != nil {
		log.Print(err)
	}
}
