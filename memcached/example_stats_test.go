package memcached_test

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/EyJunge1/go-memcached/memcached"
)

func ExampleClient_Stats() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	var got []memcached.Stat
	err = client.Stats(context.Background(), &got, "items")
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	_ = got
}

func ExampleClient_StatsAll() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	var all []memcached.NodeStats
	err = client.StatsAll(context.Background(), &all)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	for _, ns := range all {
		_ = ns.Server
		_ = ns.Stats
	}
}

func ExampleClient_Version() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	var ver []byte
	err = client.Version(context.Background(), &ver)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println(string(ver))
}

func ExampleClient_VersionAll() {
	client, err := memcached.New(memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	var all []memcached.NodeVersion
	err = client.VersionAll(context.Background(), &all)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	for _, nv := range all {
		_ = nv.Server
		_ = nv.Version
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
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()
}
