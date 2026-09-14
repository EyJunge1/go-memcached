package memcached_test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/EyJunge1/go-memcached/memcached"
)

func Example() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.Store(ctx, memcached.Item{Key: []byte("key"), Value: []byte("value")}); err != nil {
		log.Fatal(err)
	}
	var got memcached.GetResult
	err = client.GetOne(ctx, []byte("key"), &got, memcached.GetOpts{})
	if err != nil {
		log.Fatal(err)
	}
	_ = got
}

func ExampleNew() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		log.Fatal(err)
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
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()
}

func ExampleClient_Store() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	err = client.Store(context.Background(), memcached.Item{
		Key:   []byte("key"),
		Value: []byte("value"),
	})
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleClient_Get() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	dst := make([]memcached.GetResult, 1)
	err = client.Get(context.Background(), [][]byte{[]byte("key")}, dst, memcached.GetOpts{})
	if err != nil {
		log.Fatal(err)
	}
	_ = dst[0]
}

func ExampleClient_Close() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := client.Close(); err != nil {
		log.Fatal(err)
	}
}

func ExampleClient_Stats() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	var got []memcached.Stat
	err = client.Stats(context.Background(), &got, "items")
	if err != nil {
		log.Fatal(err)
	}
	_ = got
}

func ExampleClient_StatsAll() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	var all []memcached.NodeStats
	err = client.StatsAll(context.Background(), &all)
	if err != nil {
		log.Fatal(err)
	}
	for _, ns := range all {
		_ = ns.Server
		_ = ns.Stats
	}
}

func ExamplePipeline_Run() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	err = client.Pipeline().
		Store(memcached.Item{Key: []byte("k"), Value: []byte("v")}).
		Touch([]byte("k"), memcached.Rel(time.Minute)).
		Run(context.Background())
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleClient_Admin() {
	client, err := memcached.New(memcached.Config{
		Servers:    []memcached.Server{{Address: "localhost:11211"}},
		AllowAdmin: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	admin, err := client.Admin()
	if err != nil {
		log.Fatal(err)
	}
	_ = admin.On(client.Servers()[0])
	_ = admin.All()
}

func ExampleAdminNode_Flush() {
	client, err := memcached.New(memcached.Config{
		Servers:       []memcached.Server{{Address: "localhost:11211"}},
		AllowAdmin:    true,
		AllowFlushAll: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	admin, err := client.Admin()
	if err != nil {
		log.Fatal(err)
	}
	if err := admin.On(client.Servers()[0]).Flush(context.Background(), memcached.FlushOpts{}); err != nil {
		log.Fatal(err)
	}
}

func ExampleAdminBroadcast_Flush() {
	client, err := memcached.New(memcached.Config{
		Servers:       []memcached.Server{{Address: "localhost:11211"}},
		AllowAdmin:    true,
		AllowFlushAll: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	admin, err := client.Admin()
	if err != nil {
		log.Fatal(err)
	}
	if err := admin.All().Flush(context.Background(), memcached.FlushOpts{}); err != nil {
		log.Fatal(err)
	}
}

func ExampleAdminNode_Watch() {
	client, err := memcached.New(memcached.Config{
		Servers:    []memcached.Server{{Address: "localhost:11211"}},
		AllowAdmin: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	admin, err := client.Admin()
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	events, err := admin.On(client.Servers()[0]).Watch(ctx, memcached.WatchFetchers)
	if err != nil {
		log.Fatal(err)
	}
	for ev := range events {
		if ev.Err != nil {
			break
		}
	}
}

func ExampleRel() {
	_ = memcached.Item{Key: []byte("k"), Value: []byte("v"), Exp: memcached.Rel(30 * time.Second)}
}

func ExampleParseServers() {
	servers, err := memcached.ParseServers("localhost:11211,unix:///tmp/memcached.sock?weight=2")
	if err != nil {
		log.Fatal(err)
	}
	_ = servers
}

func ExampleConfig_failover() {
	_ = memcached.Config{
		Servers:  []memcached.Server{{Address: "localhost:11211"}},
		Failover: memcached.DefaultFailover(),
	}
}

func ExampleClient_Delete() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	if err := client.Delete(context.Background(), []byte("key"), memcached.MutateOpts{}); err != nil {
		log.Fatal(err)
	}
}

func ExampleClient_Incr() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	n, err := client.Incr(context.Background(), []byte("n"), 1)
	if err != nil {
		log.Fatal(err)
	}
	_ = n
}

func ExampleClient_Version() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	var ver []byte
	err = client.Version(context.Background(), &ver)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(ver))
}

func ExampleClient_VersionAll() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	var all []memcached.NodeVersion
	err = client.VersionAll(context.Background(), &all)
	if err != nil {
		log.Fatal(err)
	}
	for _, nv := range all {
		_ = nv.Server
		_ = nv.Version
	}
}

func ExampleMetaClient_Get() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	var res memcached.MetaResult
	err = client.Meta().Get(context.Background(), []byte("key"), &res, memcached.MetaGet{
		Return: memcached.MetaGetReturn{Value: true},
	})
	if err != nil {
		log.Fatal(err)
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
		log.Fatal(err)
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
		log.Fatal(err)
	}
	if res.Item.Won {
		_ = client.Meta().Set(ctx, []byte("key"), []byte("fresh"), &res, memcached.MetaSet{})
	}
}

func ExampleMetaClient_Pipeline() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	ops := []memcached.MetaOp{
		{Key: []byte("a"), Get: &memcached.MetaGet{Return: memcached.MetaGetReturn{Value: true}}},
		{Key: []byte("b"), Set: &memcached.MetaSet{}, Body: []byte("v")},
	}
	got := make([]memcached.MetaResult, len(ops))
	err = client.Meta().Pipeline(context.Background(), ops, got)
	if err != nil {
		log.Fatal(err)
	}
	_ = got
}

func ExampleAdminNode_StatsDetail() {
	client, err := memcached.New(memcached.Config{
		Servers:    []memcached.Server{{Address: "localhost:11211"}},
		AllowAdmin: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	admin, err := client.Admin()
	if err != nil {
		log.Fatal(err)
	}
	var got []memcached.Stat
	err = admin.On(client.Servers()[0]).StatsDetail(context.Background(), &got, memcached.StatsDetailDump)
	if err != nil {
		log.Fatal(err)
	}
	_ = got
}

func ExampleAdminNode_LRUCrawlerMetaDump() {
	client, err := memcached.New(memcached.Config{
		Servers:    []memcached.Server{{Address: "localhost:11211"}},
		AllowAdmin: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	admin, err := client.Admin()
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ch, err := admin.On(client.Servers()[0]).LRUCrawlerMetaDump(ctx, "all")
	if err != nil {
		log.Fatal(err)
	}
	for e := range ch {
		if e.Err != nil {
			break
		}
	}
}

func ExampleUnixExp() {
	exp, err := memcached.UnixExp(time.Now().Add(time.Hour))
	if err != nil {
		log.Fatal(err)
	}
	_ = memcached.Item{Key: []byte("k"), Value: []byte("v"), Exp: exp}
}

func ExampleDefaultFailover() {
	_ = memcached.DefaultFailover()
}

func ExampleClient_GetOne() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	var got memcached.GetResult
	err = client.GetOne(context.Background(), []byte("key"), &got, memcached.GetOpts{})
	if err != nil {
		log.Fatal(err)
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
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	dst := make([]memcached.GetResult, 1)
	err = client.Get(context.Background(), [][]byte{[]byte("key")}, dst, memcached.GetOpts{})
	if err != nil {
		log.Fatal(err)
	}
	if dst[0].Found {
		_ = dst[0].Value
	}
}

func ExampleTransport_TLS() {
	cfg := memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
		Transport: memcached.Transport{
			TLS: &tls.Config{MinVersion: tls.VersionTLS12, ServerName: "localhost"},
		},
	}
	client, err := memcached.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()
}

func ExampleClient_Touch() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	err = client.Touch(context.Background(), []byte("key"), memcached.Rel(time.Minute), memcached.MutateOpts{})
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleClient_Store_errorsIs() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		log.Fatal(err)
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
		log.Fatal(err)
	}
}

func ExampleClient_Incr_afterStore() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	if err := client.Store(ctx, memcached.Item{Key: []byte("n"), Value: []byte("10")}); err != nil {
		log.Fatal(err)
	}
	n, err := client.Incr(ctx, []byte("n"), 1)
	if err != nil {
		log.Fatal(err)
	}
	_ = n
}

func ExampleMetaClient_Set() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	var res memcached.MetaResult
	err = client.Meta().Set(context.Background(), []byte("key"), []byte("value"), &res, memcached.MetaSet{
		Return: memcached.MetaSetReturn{CAS: true},
	})
	if err != nil {
		log.Fatal(err)
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
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	var res memcached.MetaResult
	err = client.Meta().Delete(context.Background(), []byte("key"), &res, memcached.MetaDelete{})
	if err != nil {
		log.Fatal(err)
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
		log.Fatal(err)
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
		log.Fatal(err)
	}
	_ = res.Item.Value
}

func ExampleMetaClient_Debug() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	var dst memcached.MetaItemDebug
	err = client.Meta().Debug(context.Background(), []byte("key"), &dst, memcached.MetaDebug{})
	if err != nil {
		log.Fatal(err)
	}
	if err := dst.Err(); err != nil {
		log.Print(err)
	}
}

func ExampleAdminNode_StatsReset() {
	client, err := memcached.New(memcached.Config{
		Servers:    []memcached.Server{{Address: "localhost:11211"}},
		AllowAdmin: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	admin, err := client.Admin()
	if err != nil {
		log.Fatal(err)
	}
	if err := admin.On(client.Servers()[0]).StatsReset(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func ExampleAdminNode_StatsSizes() {
	client, err := memcached.New(memcached.Config{
		Servers:    []memcached.Server{{Address: "localhost:11211"}},
		AllowAdmin: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	admin, err := client.Admin()
	if err != nil {
		log.Fatal(err)
	}
	var got []memcached.Stat
	if err := admin.On(client.Servers()[0]).StatsSizes(context.Background(), &got); err != nil {
		log.Fatal(err)
	}
	_ = got
}
