package memcached_test

import (
	"context"
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
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.Store(ctx, memcached.Item{Key: []byte("key"), Value: []byte("value")}); err != nil {
		log.Fatal(err)
	}
	items, err := client.Get(ctx, [][]byte{[]byte("key")}, memcached.GetOpts{})
	if err != nil {
		log.Fatal(err)
	}
	_ = items
}

func ExampleNew() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
}

func ExampleConfig() {
	cfg := memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	}
	_ = cfg
}

func ExampleClient_Store() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

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
	defer client.Close()

	items, err := client.Get(context.Background(), [][]byte{[]byte("key")}, memcached.GetOpts{})
	if err != nil {
		log.Fatal(err)
	}
	_ = items
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
	defer client.Close()

	got, err := client.Stats(context.Background(), "items")
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
	defer client.Close()

	all, err := client.StatsAll(context.Background())
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
	defer client.Close()

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
	defer client.Close()

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
	defer client.Close()

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
	defer client.Close()

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
	defer client.Close()

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
	defer client.Close()

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
	defer client.Close()

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
	defer client.Close()

	ver, err := client.Version(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(ver)
}

func ExampleClient_VersionAll() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	all, err := client.VersionAll(context.Background())
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
	defer client.Close()

	res, err := client.Meta().Get(context.Background(), []byte("key"), memcached.MetaGet{
		Return: memcached.MetaGetReturn{Value: true},
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := res.Err(); err != nil {
		log.Fatal(err)
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
	defer client.Close()

	ctx := context.Background()
	vivify := memcached.Rel(30 * time.Second)
	res, err := client.Meta().Get(ctx, []byte("key"), memcached.MetaGet{
		Vivify:  &vivify,
		Recache: 10 * time.Second,
		Return:  memcached.MetaGetReturn{Value: true},
	})
	if err != nil {
		log.Fatal(err)
	}
	if res.Item.Won {
		_, _ = client.Meta().Set(ctx, []byte("key"), []byte("fresh"), memcached.MetaSet{})
	}
}

func ExampleMetaClient_Pipeline() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	got, err := client.Meta().Pipeline(context.Background(), []memcached.MetaOp{
		{Key: []byte("a"), Get: &memcached.MetaGet{Return: memcached.MetaGetReturn{Value: true}}},
		{Key: []byte("b"), Set: &memcached.MetaSet{}, Body: []byte("v")},
	})
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
	defer client.Close()

	admin, err := client.Admin()
	if err != nil {
		log.Fatal(err)
	}
	got, err := admin.On(client.Servers()[0]).StatsDetail(context.Background(), memcached.StatsDetailDump)
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
	defer client.Close()

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
