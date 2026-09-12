package memcached_test

import (
	"context"
	"fmt"

	"github.com/EyJunge1/go-memcached/memcached"
)

func ExamplePipeline_Run() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	var stored, fetched memcached.PipelineResult
	var got memcached.GetResult
	err = client.Pipeline().
		Store(memcached.Item{Key: []byte("k"), Value: []byte("v")}, &stored).
		GetOne([]byte("k"), &got, memcached.GetOpts{}, &fetched).
		Run(context.Background())
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	if stored.Err != nil || fetched.Err != nil || !got.Found {
		fmt.Println("error: pipeline operation failed")
		return
	}
}

func ExamplePipeline_Prepare() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	var got memcached.GetResult
	batch, err := client.Pipeline().
		Store(memcached.Item{Key: []byte("k"), Value: []byte("v")}).
		GetOne([]byte("k"), &got, memcached.GetOpts{}).
		Prepare()
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = batch.Close() }()
	if err = batch.Run(context.Background()); err != nil {
		fmt.Println("error:", err)
		return
	}
}

func ExamplePreparedBatch_RunValues() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	batch, err := client.Pipeline().
		Store(memcached.Item{Key: []byte("a")}).
		Store(memcached.Item{Key: []byte("b")}).
		Prepare()
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = batch.Close() }()
	err = batch.RunValues(context.Background(), [][]byte{
		[]byte("dynamic-a"),
		[]byte("dynamic-b"),
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
}

func ExampleClient_Session() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	session, err := client.Session(context.Background(), []byte("k"))
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = session.Close() }()
	if err = session.Store(context.Background(), memcached.Item{
		Key: []byte("k"), Value: []byte("v"),
	}); err != nil {
		fmt.Println("error:", err)
		return
	}
}

func ExampleClient_NewSessionPool() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	pool, err := client.NewSessionPool(context.Background(), []byte("worker-key"), 4)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = pool.Close() }()
	err = pool.Do(context.Background(), func(session *memcached.Session) error {
		return session.Store(context.Background(), memcached.Item{
			Key: []byte("worker-key"), Value: []byte("v"),
		})
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
}

func ExampleMetaClient_Pipeline() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	ops := []memcached.MetaOp{
		{Key: []byte("a"), Get: &memcached.MetaGet{Return: memcached.MetaGetReturn{Value: true}}},
		{Key: []byte("b"), Set: &memcached.MetaSet{}, Body: []byte("v")},
	}
	got := make([]memcached.MetaResult, len(ops))
	err = client.Meta().Pipeline(context.Background(), ops, got)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	_ = got
}

func ExampleMetaClient_PreparePipeline() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	ops := []memcached.MetaOp{
		{Key: []byte("k"), Set: &memcached.MetaSet{}},
		{Key: []byte("k"), Get: &memcached.MetaGet{Return: memcached.MetaGetReturn{Value: true}}},
	}
	got := make([]memcached.MetaResult, len(ops))
	batch, err := client.Meta().PreparePipeline(ops, got)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = batch.Close() }()
	if err = batch.RunValues(context.Background(), [][]byte{[]byte("dynamic")}); err != nil {
		fmt.Println("error:", err)
		return
	}
}
