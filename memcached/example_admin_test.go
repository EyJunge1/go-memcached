package memcached_test

import (
	"context"
	"fmt"
	"time"

	"github.com/EyJunge1/go-memcached/memcached"
)

func ExampleClient_Admin() {
	client, err := memcached.New(memcached.Config{
		Servers:    []memcached.Server{{Address: "localhost:11211"}},
		AllowAdmin: true,
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	admin, err := client.Admin()
	if err != nil {
		fmt.Println("error:", err)
		return
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
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	admin, err := client.Admin()
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	if err := admin.On(client.Servers()[0]).Flush(context.Background(), memcached.FlushOpts{}); err != nil {
		fmt.Println("error:", err)
		return
	}
}

func ExampleAdminBroadcast_Flush() {
	client, err := memcached.New(memcached.Config{
		Servers:       []memcached.Server{{Address: "localhost:11211"}},
		AllowAdmin:    true,
		AllowFlushAll: true,
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	admin, err := client.Admin()
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	if err := admin.All().Flush(context.Background(), memcached.FlushOpts{}); err != nil {
		fmt.Println("error:", err)
		return
	}
}

func ExampleAdminNode_Watch() {
	client, err := memcached.New(memcached.Config{
		Servers:    []memcached.Server{{Address: "localhost:11211"}},
		AllowAdmin: true,
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	admin, err := client.Admin()
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, err := admin.On(client.Servers()[0]).Watch(ctx, memcached.WatchFetchers)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer stream.Close()
	for ev := range stream.Events {
		if ev.Err != nil {
			break
		}
	}
}

func ExampleAdminNode_StatsDetail() {
	client, err := memcached.New(memcached.Config{
		Servers:    []memcached.Server{{Address: "localhost:11211"}},
		AllowAdmin: true,
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	admin, err := client.Admin()
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	var got []memcached.Stat
	err = admin.On(client.Servers()[0]).StatsDetail(context.Background(), &got, memcached.StatsDetailDump)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	_ = got
}

func ExampleAdminNode_LRUCrawlerMetaDump() {
	client, err := memcached.New(memcached.Config{
		Servers:    []memcached.Server{{Address: "localhost:11211"}},
		AllowAdmin: true,
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	admin, err := client.Admin()
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, err := admin.On(client.Servers()[0]).LRUCrawlerMetaDump(ctx, "all")
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer stream.Close()
	for e := range stream.Entries {
		if e.Err != nil {
			break
		}
	}
}

func ExampleAdminNode_StatsReset() {
	client, err := memcached.New(memcached.Config{
		Servers:    []memcached.Server{{Address: "localhost:11211"}},
		AllowAdmin: true,
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	admin, err := client.Admin()
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	if err := admin.On(client.Servers()[0]).StatsReset(context.Background()); err != nil {
		fmt.Println("error:", err)
		return
	}
}

func ExampleAdminNode_StatsSizes() {
	client, err := memcached.New(memcached.Config{
		Servers:    []memcached.Server{{Address: "localhost:11211"}},
		AllowAdmin: true,
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	admin, err := client.Admin()
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	var got []memcached.Stat
	if err := admin.On(client.Servers()[0]).StatsSizes(context.Background(), &got); err != nil {
		fmt.Println("error:", err)
		return
	}
	_ = got
}
