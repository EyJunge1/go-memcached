package memcached

import (
	"context"
	"testing"
)

type firstSelector struct{}

func (firstSelector) PickServers(servers []Server, _ []byte) (Server, error) {
	return servers[0], nil
}

func TestCustomServerSelector(t *testing.T) {
	s1 := startFakeMemcached(t)
	s2 := startFakeMemcached(t)
	c := mustNewClient(t, Config{
		Servers: []Server{{Address: s1.Addr()}, {Address: s2.Addr()}},
		Cluster: Cluster{Selector: firstSelector{}},
	})
	t.Cleanup(func() { _ = c.Close() })

	ctx := context.Background()
	for i := 0; i < 20; i++ {
		key := []byte{byte('a' + i%26)}
		noError(t, c.Store(ctx, Item{Key: key, Value: []byte("v")}))
	}
	// All keys on first server: second fake should be empty for gets via On.
	n2 := c.On(c.Servers()[1])
	var got GetResult
	noError(t, n2.GetOne(ctx, []byte("a"), &got, GetOpts{}))
	isFalse(t, got.Found)
	n1 := c.On(c.Servers()[0])
	noError(t, n1.GetOne(ctx, []byte("a"), &got, GetOpts{}))
	isTrue(t, got.Found)
}
