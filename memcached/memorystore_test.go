package memcached

import (
	"bufio"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestParseMemorystoreConfig(t *testing.T) {
	const body = "CONFIG cluster 0 64\r\n" +
		"3\r\n" +
		"host1|10.0.0.1|11211 host2|10.0.0.2|11211\r\n" +
		"\r\n" +
		"END\r\n"
	cfg, err := parseMemorystoreConfig(bufio.NewReader(strings.NewReader(body)))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConfigID != 3 {
		t.Fatalf("ConfigID=%d", cfg.ConfigID)
	}
	if len(cfg.Nodes) != 2 {
		t.Fatalf("nodes=%d", len(cfg.Nodes))
	}
	if cfg.Nodes[0].Host != "10.0.0.1" || cfg.Nodes[0].Port != 11211 {
		t.Fatalf("node0=%+v", cfg.Nodes[0])
	}
	if cfg.Nodes[1].Host != "10.0.0.2" || cfg.Nodes[1].Port != 11211 {
		t.Fatalf("node1=%+v", cfg.Nodes[1])
	}
}

func TestParseMemorystoreConfigHostnameFallback(t *testing.T) {
	const body = "CONFIG cluster 0 32\r\n" +
		"1\r\n" +
		"onlyhost||11211\r\n" +
		"END\r\n"
	cfg, err := parseMemorystoreConfig(bufio.NewReader(strings.NewReader(body)))
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Nodes) != 1 || cfg.Nodes[0].Host != "onlyhost" {
		t.Fatalf("nodes=%+v", cfg.Nodes)
	}
}

func TestNewRejectsMemorystoreWithoutRefresh(t *testing.T) {
	_, err := New(Config{
		Servers:         []Server{{Address: "127.0.0.1:11211"}},
		AllowSetServers: true,
		Cluster:         Cluster{MemorystoreDiscovery: true},
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestNewRejectsMemorystoreRefreshUnderOneSecond(t *testing.T) {
	_, err := New(Config{
		Servers:         []Server{{Address: "127.0.0.1:11211"}},
		AllowSetServers: true,
		Cluster: Cluster{
			MemorystoreDiscovery: true,
			ResolverRefresh:      500 * time.Millisecond,
		},
	})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("got %v", err)
	}
}

func TestNewMemorystoreSetsResolver(t *testing.T) {
	disc := startFakeDiscovery(t)
	node := startFakeMemcached(t)
	disc.setConfig(1, []string{node.Addr()})

	c, err := New(Config{
		Servers:         []Server{{Address: "127.0.0.1:1"}}, // seed replaced by eager resolve
		AllowSetServers: true,
		Cluster: Cluster{
			MemorystoreDiscovery: true,
			ResolverRefresh:      time.Hour,
			ResolveSpec:          disc.Addr(),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if _, ok := c.rt.resolver.(*memorystoreResolver); !ok {
		t.Fatalf("resolver=%T", c.rt.resolver)
	}
	sv := c.Servers()
	if len(sv) != 1 || sv[0].Address != node.Addr() {
		t.Fatalf("servers=%v want %s", sv, node.Addr())
	}
	if err := c.ResolverErr(); err != nil {
		t.Fatalf("ResolverErr=%v", err)
	}
}

func TestMemorystoreEagerResolveKeepsSeedOnFailure(t *testing.T) {
	seed := startFakeMemcached(t)
	c, err := New(Config{
		Servers:         []Server{{Address: seed.Addr()}},
		AllowSetServers: true,
		Cluster: Cluster{
			MemorystoreDiscovery: true,
			ResolverRefresh:      time.Hour,
			ResolveSpec:          "127.0.0.1:1", // nothing listening
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	sv := c.Servers()
	if len(sv) != 1 || sv[0].Address != seed.Addr() {
		t.Fatalf("seed not kept: %v", sv)
	}
	if c.ResolverErr() == nil {
		t.Fatal("expected ResolverErr after failed eager resolve")
	}
}

func TestMemorystoreConfigIDCommitsOnlyAfterApply(t *testing.T) {
	disc := startFakeDiscovery(t)
	n1 := startFakeMemcached(t)
	disc.setConfig(1, []string{n1.Addr()})

	rt := &runtime{dialTimeout: time.Second, readTimeout: time.Second}
	ms := newMemorystoreResolver(disc.Addr(), rt)
	addrs, err := ms.Resolve("")
	if err != nil {
		t.Fatal(err)
	}
	if len(addrs) != 1 {
		t.Fatalf("addrs=%d", len(addrs))
	}
	if ms.configID.Load() != 0 {
		t.Fatalf("configID committed early: %d", ms.configID.Load())
	}
	ms.clearPending()
	if ms.configID.Load() != 0 {
		t.Fatal("clearPending should not commit")
	}
	// Retry same ConfigID after rollback still returns pending (ID not committed).
	addrs, err = ms.Resolve("")
	if err != nil {
		t.Fatal(err)
	}
	ms.commitPending()
	if ms.configID.Load() != 1 {
		t.Fatalf("configID=%d after commit", ms.configID.Load())
	}
	_ = addrs
}
