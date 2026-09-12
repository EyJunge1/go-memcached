package memcached

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestStatsClientErrorPublic(t *testing.T) {
	addr, dials := startCmdServer(t, func(fields []string) string {
		switch fields[0] {
		case "stats":
			return "CLIENT_ERROR bad flags\r\n"
		case "version":
			return "VERSION after-stats\r\n"
		default:
			return "ERROR\r\n"
		}
	})
	client := testClient(t, addr)
	_, err := doStatsAll(context.Background(), client)
	isErrorIs(t, err, ErrClientError)
	contains(t, errText(t, err), "bad flags")
	isFalse(t, errors.Is(err, ErrTransport))
	ver, err := doVersion(context.Background(), client)
	noError(t, err)
	equal(t, []byte("after-stats"), ver)
	equal(t, int32(1), dials.Load())
}

func TestWatchErrorAfterOK(t *testing.T) {
	ln := mustListen(t, "tcp", "127.0.0.1:0")
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		cn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = cn.Close() }()
		buf := make([]byte, 64)
		_, _ = cn.Read(buf)
		_, _ = cn.Write([]byte("OK\r\nERROR cannot pipeline other commands before watch\r\n"))
	}()
	client := testClient(t, ln.Addr().String(), func(cfg *Config) { cfg.AllowAdmin = true })
	admin := mustAdminOn(t, client)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	watch := mustWatch(ctx, t, admin, WatchFetchers)
	defer watch.Close()
	ev := <-watch.Events
	isErrorIs(t, ev.Err, ErrCommandError)
	contains(t, ev.Err.Error(), "cannot pipeline")
}

func TestDumpMidStreamCommandError(t *testing.T) {
	ln := mustListen(t, "tcp", "127.0.0.1:0")
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		cn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = cn.Close() }()
		buf := make([]byte, 64)
		_, _ = cn.Read(buf)
		_, _ = cn.Write([]byte("key=foo exp=-1 la=0 cas=1 fetch=no cls=1 size=3\r\nERROR locked try again later\r\n"))
	}()
	client := testClient(t, ln.Addr().String(), func(cfg *Config) { cfg.AllowAdmin = true })
	admin := mustAdminOn(t, client)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	dump := mustMetaDump(ctx, t, admin, "all")
	defer dump.Close()
	row := <-dump.Entries
	equal(t, "foo", row.Fields["key"])
	isNil(t, row.Err)
	fail := <-dump.Entries
	isErrorIs(t, fail.Err, ErrCommandError)
	contains(t, fail.Err.Error(), "locked")
}

func TestAdminAllJoinsBusyAndServerError(t *testing.T) {
	busy, _ := startCmdServer(t, func(_ []string) string {
		return "BUSY crawler\r\n"
	})
	oom, _ := startCmdServer(t, func(_ []string) string {
		return "SERVER_ERROR oom\r\n"
	})
	cfg, err := testConfig(busy, func(c *Config) { c.AllowAdmin = true })
	noError(t, err)
	more, err := ParseServers(oom)
	noError(t, err)
	cfg.Servers = append(cfg.Servers, more...)
	client := mustNewClient(t, cfg)
	t.Cleanup(func() { _ = client.Close() })
	err = mustAdmin(t, client).All().Verbosity(context.Background(), 1, AdminOpts{})
	isErrorIs(t, err, ErrBusy)
	isErrorIs(t, err, ErrServerError)
	contains(t, errText(t, err), "crawler")
	contains(t, errText(t, err), "oom")
}

func TestAdminPublicWireSentinels(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name   string
		reply  string
		want   error
		run    func(*AdminNode) error
		pooled bool
	}{
		{
			name:   "badclass",
			reply:  "BADCLASS invalid src or dst class id\r\n",
			want:   ErrBadClass,
			run:    func(a *AdminNode) error { return a.SlabsReassign(ctx, 1, 2) },
			pooled: true,
		},
		{
			name:   "nospare",
			reply:  "NOSPARE source class has no spare pages\r\n",
			want:   ErrNoSpare,
			run:    func(a *AdminNode) error { return a.SlabsReassign(ctx, 1, 2) },
			pooled: true,
		},
		{
			name:   "notfull",
			reply:  "NOTFULL dest class must be full to steal from\r\n",
			want:   ErrNotFull,
			run:    func(a *AdminNode) error { return a.SlabsReassign(ctx, 1, 2) },
			pooled: true,
		},
		{
			name:   "unsafe",
			reply:  "UNSAFE source class cannot move pages\r\n",
			want:   ErrUnsafe,
			run:    func(a *AdminNode) error { return a.SlabsReassign(ctx, 1, 2) },
			pooled: true,
		},
		{
			name:   "memlimit-small",
			reply:  "MEMLIMIT_TOO_SMALL cannot set maxbytes to less than 8m\r\n",
			want:   ErrMemlimitTooSmall,
			run:    func(a *AdminNode) error { return a.CacheMemLimit(ctx, 4, AdminOpts{}) },
			pooled: true,
		},
		{
			name:   "memlimit-adjust",
			reply:  "MEMLIMIT_ADJUST_FAILED out of bounds\r\n",
			want:   ErrMemlimitAdjustFailed,
			run:    func(a *AdminNode) error { return a.CacheMemLimit(ctx, 64, AdminOpts{}) },
			pooled: true,
		},
		{
			name:   "notstarted",
			reply:  "NOTSTARTED no items to crawl\r\n",
			want:   ErrNotStarted,
			run:    func(a *AdminNode) error { return a.LRUCrawlerCrawl(ctx, "all") },
			pooled: true,
		},
		{
			name:  "watcher-too-many",
			reply: "WATCHER_TOO_MANY log watcher limit reached\r\n",
			want:  ErrWatcherTooMany,
			run: func(a *AdminNode) error {
				wctx, cancel := context.WithTimeout(ctx, 2*time.Second)
				defer cancel()
				_, err := a.Watch(wctx, WatchFetchers)
				return err
			},
		},
		{
			name:  "watcher-failed",
			reply: "WATCHER_FAILED failed to add log watcher\r\n",
			want:  ErrWatcherFailed,
			run: func(a *AdminNode) error {
				wctx, cancel := context.WithTimeout(ctx, 2*time.Second)
				defer cancel()
				_, err := a.Watch(wctx, WatchFetchers)
				return err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, dials := startCmdServer(t, func(fields []string) string {
				if fields[0] == "version" {
					return "VERSION after-admin\r\n"
				}
				return tt.reply
			})
			client := testClient(t, addr, func(c *Config) { c.AllowAdmin = true })
			err := tt.run(mustAdminOn(t, client))
			isErrorIs(t, err, tt.want)
			isFalse(t, errors.Is(err, ErrMalformedResponse))
			isFalse(t, shouldDiscardConn(err))
			contains(t, errText(t, err), strings.Fields(tt.reply)[0])
			ver, err := doVersion(ctx, client)
			noError(t, err)
			equal(t, []byte("after-admin"), ver)
			if tt.pooled && dials.Load() != 1 {
				t.Fatalf("keepalive admin sentinel discarded conn; dials=%d", dials.Load())
			}
		})
	}
}

func TestAppStatsPrefixIsNotMalformed(t *testing.T) {
	addr, dials := startCmdServer(t, func(fields []string) string {
		switch fields[0] {
		case "stats":
			return "PREFIX foo 1 2 3\r\nEND\r\n"
		case "version":
			return "VERSION after-prefix\r\n"
		default:
			return "ERROR\r\n"
		}
	})
	client := testClient(t, addr)
	all, err := doStatsAll(context.Background(), client)
	noError(t, err)
	hasLen(t, all, 1)
	for _, got := range all {
		equal(t, 0, len(got.Stats))
	}
	ver, err := doVersion(context.Background(), client)
	noError(t, err)
	equal(t, []byte("after-prefix"), ver)
	equal(t, int32(1), dials.Load())
}

func TestShutdownCloseWithoutOK(t *testing.T) {
	addr, _ := startCmdServer(t, func(_ []string) string {
		return "ERROR unexpected\r\n"
	})
	client := testClient(t, addr, func(c *Config) { c.AllowAdmin = true })
	noError(t, mustAdminOn(t, client).Shutdown(context.Background(), ShutdownImmediate))
}

func TestVersionAllJoinsServerError(t *testing.T) {
	good, _ := startCmdServer(t, func(_ []string) string {
		return "VERSION good-1\r\n"
	})
	bad, _ := startCmdServer(t, func(_ []string) string {
		return "SERVER_ERROR oom\r\n"
	})
	cfg, err := testConfig(good)
	noError(t, err)
	more, err := ParseServers(bad)
	noError(t, err)
	cfg.Servers = append(cfg.Servers, more...)
	client := mustNewClient(t, cfg)
	t.Cleanup(func() { _ = client.Close() })
	got, err := doVersionAll(context.Background(), client)
	isErrorIs(t, err, ErrServerError)
	equal(t, 1, len(got))
	for _, v := range got {
		equal(t, []byte("good-1"), v.Version)
	}
}

func TestInvalidNetworkProtocol(t *testing.T) {
	_, err := New(Config{Servers: []Server{{Network: "foo", Address: "127.0.0.1:1"}}})
	isErrorIs(t, err, ErrInvalidNetworkProtocol)
}

func TestUnknownEndIndicatorIsMalformed(t *testing.T) {
	resp := &response{endIndicator: responseEndIndicator(99)}
	err := resp.recv(context.Background(), newMockConn(), 0)
	isErrorIs(t, err, ErrMalformedResponse)
	isTrue(t, shouldDiscardConn(err))
}
