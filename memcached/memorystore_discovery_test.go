package memcached

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMemorystoreDiscoveryE2E(t *testing.T) {
	disc := startFakeDiscovery(t)
	n1 := startFakeMemcached(t)
	n2 := startFakeMemcached(t)
	disc.setConfig(1, []string{n1.Addr()})

	obs := &recordingObserver{}
	c, err := New(Config{
		Servers:         []Server{{Address: "127.0.0.1:1"}},
		AllowSetServers: true,
		Observer:        obs,
		Cluster: Cluster{
			MemorystoreDiscovery: true,
			ResolverRefresh:      time.Hour, // manual refresh via refreshMembership
			ResolveSpec:          disc.Addr(),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	if got := c.Servers(); len(got) != 1 || got[0].Address != n1.Addr() {
		t.Fatalf("eager servers=%v", got)
	}
	ms, ok := c.rt.resolver.(*memorystoreResolver)
	if !ok {
		t.Fatal("expected memorystoreResolver")
	}
	if ms.configID.Load() != 1 {
		t.Fatalf("configID=%d", ms.configID.Load())
	}

	// Same ConfigID: membership unchanged, ID stays committed.
	disc.setConfig(1, []string{n1.Addr()})
	c.refreshMembership(false)
	if got := c.Servers(); len(got) != 1 || got[0].Address != n1.Addr() {
		t.Fatalf("same-id servers=%v", got)
	}

	// ConfigID regression: keep prior membership.
	disc.setConfig(0, []string{n2.Addr()})
	c.refreshMembership(false)
	if got := c.Servers(); len(got) != 1 || got[0].Address != n1.Addr() {
		t.Fatalf("regress servers=%v", got)
	}

	// ConfigID advance: swap nodes.
	disc.setConfig(2, []string{n2.Addr()})
	c.refreshMembership(false)
	if got := c.Servers(); len(got) != 1 || got[0].Address != n2.Addr() {
		t.Fatalf("advance servers=%v", got)
	}
	if ms.configID.Load() != 2 {
		t.Fatalf("configID after advance=%d", ms.configID.Load())
	}

	// Broken payload: stand remains, ResolverErr set.
	disc.setBroken()
	c.refreshMembership(false)
	if got := c.Servers(); len(got) != 1 || got[0].Address != n2.Addr() {
		t.Fatalf("broken servers=%v", got)
	}
	if c.ResolverErr() == nil {
		t.Fatal("expected ResolverErr on broken payload")
	}

	// Empty nodes: stand remains.
	disc.setRaw("CONFIG cluster 0 8\r\n9\r\n\r\nEND\r\n")
	c.refreshMembership(false)
	if got := c.Servers(); len(got) != 1 || got[0].Address != n2.Addr() {
		t.Fatalf("empty servers=%v", got)
	}

	// Poll dial error: stand remains.
	disc.closeListener()
	c.refreshMembership(false)
	if got := c.Servers(); len(got) != 1 || got[0].Address != n2.Addr() {
		t.Fatalf("dial-fail servers=%v", got)
	}

	evs := obs.snapshot()
	var sawDiscovery bool
	for _, e := range evs {
		if e.Op == OpDiscovery {
			sawDiscovery = true
			break
		}
	}
	if !sawDiscovery {
		t.Fatal("expected discovery OpEvent")
	}
}

type fakeDiscovery struct {
	ln      net.Listener
	wg      sync.WaitGroup
	mu      sync.Mutex
	payload []byte
	closed  atomic.Bool
}

func (f *fakeDiscovery) Addr() string {
	return f.ln.Addr().String()
}

func (f *fakeDiscovery) closeListener() {
	if f.closed.Swap(true) {
		return
	}
	_ = f.ln.Close()
}

func (f *fakeDiscovery) setConfig(id int64, nodes []string) {
	parts := make([]string, 0, len(nodes))
	for _, n := range nodes {
		host, port, err := net.SplitHostPort(n)
		if err != nil {
			host, port = n, "11211"
		}
		parts = append(parts, host+"|"+host+"|"+port)
	}
	body := strconv.FormatInt(id, 10) + "\r\n" + strings.Join(parts, " ") + "\r\n\r\n"
	payload := "CONFIG cluster 0 " + strconv.Itoa(len(body)) + "\r\n" + body + "END\r\n"
	f.setRaw(payload)
}

func (f *fakeDiscovery) setBroken() {
	f.setRaw("NOT CONFIG\r\n")
}

func (f *fakeDiscovery) setRaw(payload string) {
	f.mu.Lock()
	f.payload = []byte(payload)
	f.mu.Unlock()
}

func (f *fakeDiscovery) serve() {
	defer f.wg.Done()
	for {
		cn, err := f.ln.Accept()
		if err != nil {
			return
		}
		f.wg.Add(1)
		go f.handle(cn)
	}
}

func (f *fakeDiscovery) handle(cn net.Conn) {
	defer f.wg.Done()
	defer func() { _ = cn.Close() }()
	_ = cn.SetDeadline(time.Now().Add(2 * time.Second))
	r := bufio.NewReader(cn)
	line, err := r.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return
	}
	if !strings.HasPrefix(strings.TrimSpace(line), "config get cluster") {
		_, _ = io.WriteString(cn, "ERROR\r\n")
		return
	}
	f.mu.Lock()
	payload := append([]byte(nil), f.payload...)
	f.mu.Unlock()
	_, _ = cn.Write(payload)
}

func startFakeDiscovery(t testing.TB) *fakeDiscovery {
	t.Helper()
	ln := mustListen(t, "tcp", "127.0.0.1:0")
	f := &fakeDiscovery{ln: ln}
	f.setConfig(1, []string{"127.0.0.1:11211"})
	f.wg.Add(1)
	go f.serve()
	t.Cleanup(func() {
		f.closeListener()
		f.wg.Wait()
	})
	return f
}

func TestMemorystoreObserverDiscoveryEvent(t *testing.T) {
	disc := startFakeDiscovery(t)
	node := startFakeMemcached(t)
	disc.setConfig(7, []string{node.Addr()})
	obs := &recordingObserver{}
	c, err := New(Config{
		Servers:         []Server{{Address: "127.0.0.1:1"}},
		AllowSetServers: true,
		Observer:        obs,
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
	_ = context.Background()
	evs := obs.snapshot()
	found := false
	for _, e := range evs {
		if e.Op == OpDiscovery && e.ConfigID == 7 && e.Keys == 1 && e.Err == nil {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("discovery events=%v", evs)
	}
}
