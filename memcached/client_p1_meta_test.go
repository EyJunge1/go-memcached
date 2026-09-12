package memcached

import (
	"bufio"
	"context"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestMetaPipelineNoopDoesNotBreakBarrier(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("a"), Value: []byte("1")}))
	got, err := doMetaPipeline(ctx, client, []MetaOp{
		{Key: []byte("a"), Get: &MetaGet{Return: MetaGetReturn{Value: true}}},
		{Noop: &MetaNoop{ProxyHint: "hint", ProxyPath: "/path"}},
	})
	noError(t, err)
	hasLen(t, got, 2)
	equal(t, []byte("1"), got[0].Item.Value)
	equal(t, MetaStatusMN, got[1].Status)
	srv.mu.Lock()
	cmd := srv.lastCmd
	srv.mu.Unlock()
	contains(t, cmd, "mn")
	contains(t, cmd, "Phint")
	contains(t, cmd, "L/path")
	err = client.Version(ctx, new([]byte))
	noError(t, err)
}

func TestMetaSetHonorsMaxValueBytes(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) { cfg.MaxValueBytes = 4 })
	_, err := doMetaSet(context.Background(), client, []byte("k"), []byte("too-long"), MetaSet{})
	isErrorIs(t, err, ErrInvalidValue)
}

func TestNoReplyClientErrorDoesNotPoisonNextGet(t *testing.T) {
	ln := mustListen(t, "tcp", "127.0.0.1:0")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			cn, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func(cn net.Conn) {
				defer wg.Done()
				defer func() { _ = cn.Close() }()
				r := bufio.NewReader(cn)
				errReply := []byte("ERROR\r\n")
				for {
					line, err := r.ReadBytes('\n')
					if err != nil {
						return
					}
					cmd := strings.Fields(string(line))
					if len(cmd) == 0 {
						continue
					}
					switch cmd[0] {
					case "set", "add", "replace", "append", "prepend", "cas":
						if len(cmd) >= 5 {
							if nbytes, err := strconv.Atoi(cmd[4]); err == nil && nbytes > 0 {
								_, _ = io.ReadFull(r, make([]byte, nbytes+2))
							}
						}
						reply := []byte("STORED\r\n")
						if cmd[len(cmd)-1] == "noreply" {
							reply = []byte("CLIENT_ERROR bad command line\r\n")
						}
						_, _ = cn.Write(reply)
					case "get", "gets":
						_, _ = cn.Write([]byte("END\r\n"))
					case "version":
						_, _ = cn.Write([]byte("VERSION after-quiet\r\n"))
					case "quit":
						return
					default:
						_, _ = cn.Write(errReply)
					}
				}
			}(cn)
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		wg.Wait()
	})

	var dials atomic.Int32
	client := testClient(t, ln.Addr().String(), func(cfg *Config) {
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
	})
	ctx := context.Background()
	err := client.Store(ctx, Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v"), Quiet: true})
	isErrorIs(t, err, ErrClientError)
	contains(t, errText(t, err), "bad command line")
	items, err := doGet(ctx, client, [][]byte{[]byte("missing")}, GetOpts{})
	noError(t, err)
	hasLen(t, items, 1)
	isFalse(t, items[0].Found)
	ver, err := doVersion(ctx, client)
	noError(t, err)
	equal(t, []byte("after-quiet"), ver)
	if dials.Load() < 2 {
		t.Fatalf("quiet CLIENT_ERROR must discard conn; dials=%d", dials.Load())
	}
}

func TestMetaPipelineRejectsDuplicateOpaque(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	_, err := doMetaPipeline(context.Background(), client, []MetaOp{
		{Key: []byte("a"), Get: &MetaGet{Opaque: []byte("same")}},
		{Key: []byte("b"), Get: &MetaGet{Opaque: []byte("same")}},
	})
	isErrorIs(t, err, ErrInvalidArgument)
}
