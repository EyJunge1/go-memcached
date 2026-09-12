package memcached

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSetServersIdenticalMembershipKeepsSessionValid(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.AllowSetServers = true
	})
	session, err := client.Session(context.Background(), []byte("k"))
	noError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	version := client.clusterVersion.Load()

	noError(t, client.SetServers(client.Servers()))
	equal(t, version, client.clusterVersion.Load())
	noError(t, session.Store(context.Background(), Item{
		Mode: StoreSet, Key: []byte("k"), Value: []byte("v"),
	}))
}

func TestValidAuthSecretRejectsControls(t *testing.T) {
	for _, s := range []string{"a\x07b", "a\x7fb", "a b", "a\nb", "a\x00b"} {
		t.Run(s, func(t *testing.T) {
			isErrorIs(t, validAuthSecret(s), ErrInvalidArgument)
		})
	}
	noError(t, validAuthSecret("ok-user_1"))
}

func TestValidNamespaceRejectsControls(t *testing.T) {
	for _, ns := range []string{"a b", "a\nb", "a\x00b", "a\x7fb"} {
		t.Run(ns, func(t *testing.T) {
			isErrorIs(t, validNamespace(ns), ErrInvalidArgument)
		})
	}
	noError(t, validNamespace("ns:"))
	cfg, err := testConfig("127.0.0.1:11211")
	noError(t, err)
	cfg.Cluster.Namespace = "evil\r\n"
	_, err = New(cfg)
	isErrorIs(t, err, ErrInvalidArgument)
	noError(t, validNamespace(strings.Repeat("n", _maxKeySize-1)))
	isErrorIs(t, validNamespace(strings.Repeat("n", _maxKeySize)), ErrInvalidArgument)
}

func TestTextKeyAcceptsOpaqueHighBitBytes(t *testing.T) {
	noError(t, validateKeyAndValue([]byte{'k', 0x80, 0xff}, nil))
}

func TestAdversarialValueBodiesRemainLengthFramed(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	payload := []byte("\r\nquit\r\nEND\r\nVALUE forged 0 1\r\nx\r\nMN\r\n")

	noError(t, client.Store(ctx, Item{Key: []byte("plain"), Value: payload}))
	equal(t, payload, mustGet(ctx, t, client, []byte("plain")).Value)

	batch, err := client.Pipeline().
		Store(Item{Key: []byte("prepared")}).
		Prepare()
	noError(t, err)
	t.Cleanup(func() { _ = batch.Close() })
	noError(t, batch.RunValues(ctx, [][]byte{payload}))
	equal(t, payload, mustGet(ctx, t, client, []byte("prepared")).Value)

	dst := make([]MetaResult, 1)
	metaBatch, err := client.Meta().PreparePipeline([]MetaOp{{
		Key: []byte("meta"), Set: &MetaSet{},
	}}, dst)
	noError(t, err)
	t.Cleanup(func() { _ = metaBatch.Close() })
	noError(t, metaBatch.RunValues(ctx, [][]byte{payload}))
	equal(t, payload, mustGet(ctx, t, client, []byte("meta")).Value)
}

func TestValidMetaHintRejectsOverlong(t *testing.T) {
	noError(t, validMetaHint(strings.Repeat("A", _maxMetaHintBytes)))
	isErrorIs(t, validMetaHint(strings.Repeat("A", _maxMetaHintBytes+1)), ErrInvalidArgument)
}

func TestNilMetaClientReturnsErrClosed(t *testing.T) {
	var m *MetaClient
	ctx := context.Background()
	checks := []func() error{
		func() error { return m.Get(ctx, nil, nil, MetaGet{}) },
		func() error { return m.GetOne(ctx, nil, nil, MetaGet{}) },
		func() error { return m.Set(ctx, nil, nil, nil, MetaSet{}) },
		func() error { return m.Store(ctx, Item{}, nil) },
		func() error { return m.Delete(ctx, nil, nil, MetaDelete{}) },
		func() error { return m.Arithmetic(ctx, nil, 0, nil, MetaArithmetic{}) },
		func() error { return m.Debug(ctx, nil, nil, MetaDebug{}) },
		func() error { return m.Noop(ctx, nil, MetaNoop{}) },
		func() error { return m.GetMulti(ctx, nil, nil, MetaGet{}) },
		func() error { return m.StoreMulti(ctx, nil, nil) },
		func() error { return m.DeleteMulti(ctx, nil, DeleteMissIgnore, nil) },
		func() error {
			return m.GetOrFill(ctx, nil, &MetaResult{}, GetOrFillOpts{}, func(context.Context, []byte) ([]byte, uint32, Exptime, error) {
				return nil, 0, Rel(0), nil
			})
		},
		func() error { return m.Pipeline(ctx, nil, nil) },
		func() error {
			_, err := m.PreparePipeline(nil, nil)
			return err
		},
		func() error { return m.Deferred(ctx).Flush() },
	}
	for _, check := range checks {
		isErrorIs(t, check(), ErrClosed)
	}
}

func TestNewRejectsMinIdleAboveMaxOpen(t *testing.T) {
	cfg, err := testConfig("127.0.0.1:11211")
	noError(t, err)
	cfg.Pool.MinIdle = 5
	cfg.Pool.MaxOpen = 2
	_, err = New(cfg)
	isErrorIs(t, err, ErrInvalidArgument)
}

func TestNewRejectsMinIdleWhenIdlePoolDisabled(t *testing.T) {
	cfg, err := testConfig("127.0.0.1:11211")
	noError(t, err)
	cfg.Pool.MinIdle = 1
	cfg.Pool.MaxIdle = -1
	_, err = New(cfg)
	isErrorIs(t, err, ErrInvalidArgument)
}

func TestUnixSocketSymlinkRejected(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "sock")
	link := filepath.Join(dir, "link")
	ln := mustListen(t, "unix", target)
	t.Cleanup(func() { _ = ln.Close() })
	noError(t, os.Chmod(target, 0o600))
	noError(t, os.Symlink(target, link))
	isErrorIs(t, checkUnixSocketMode(link, false, 0), ErrInvalidArgument)
	isErrorIs(t, checkUnixSocketMode(link, true, 0), ErrInvalidArgument)
}
