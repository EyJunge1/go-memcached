package memcached

import (
	"errors"
	"io"
	"net"
	"syscall"
	"testing"
)

func TestBuildAdminCommandsWire(t *testing.T) {
	tests := []struct {
		name    string
		build   func() (*request, *response)
		wantRaw []byte
	}{
		{
			name:    "verbosity",
			build:   func() (*request, *response) { req, resp, _ := buildVerbosityCommand(2, nil, false); return req, resp },
			wantRaw: []byte("verbosity 2\r\n"),
		},
		{
			name:    "verbosity noreply",
			build:   func() (*request, *response) { req, resp, _ := buildVerbosityCommand(2, nil, true); return req, resp },
			wantRaw: []byte("verbosity 2 noreply\r\n"),
		},
		{
			name: "cache_memlimit noreply",
			build: func() (*request, *response) {
				req, resp, _ := buildCacheMemLimitCommand(64, nil, true)
				return req, resp
			},
			wantRaw: []byte("cache_memlimit 64 noreply\r\n"),
		},
		{
			name: "refresh_certs",
			build: func() (*request, *response) {
				req, resp, err := buildRefreshCertsCommand()
				noError(t, err)
				return req, resp
			},
			wantRaw: []byte("refresh_certs\r\n"),
		},
		{
			name: "shutdown graceful",
			build: func() (*request, *response) {
				req, resp, err := buildShutdownCommand(true)
				noError(t, err)
				return req, resp
			},
			wantRaw: []byte("shutdown graceful\r\n"),
		},
		{
			name: "slabs reassign",
			build: func() (*request, *response) {
				req, resp, err := buildSlabsReassignCommand(-1, 3)
				noError(t, err)
				return req, resp
			},
			wantRaw: []byte("slabs reassign -1 3\r\n"),
		},
		{
			name:    "lru_crawler crawl",
			build:   func() (*request, *response) { return buildLRUCrawlerCommand("crawl", "all") },
			wantRaw: []byte("lru_crawler crawl all\r\n"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, resp := tt.build()
			defer releaseReqAndResp(req, resp)
			equal(t, tt.wantRaw, req.raw)
			notNil(t, resp)
		})
	}
}

func TestBuildAdminCommandsEndIndicator(t *testing.T) {
	tests := []struct {
		name    string
		build   func() (*request, *response)
		wantEnd responseEndIndicator
	}{
		{
			name:    "verbosity noreply",
			build:   func() (*request, *response) { req, resp, _ := buildVerbosityCommand(2, nil, true); return req, resp },
			wantEnd: _endIndicatorNoReply,
		},
		{
			name: "refresh_certs",
			build: func() (*request, *response) {
				req, resp, err := buildRefreshCertsCommand()
				noError(t, err)
				return req, resp
			},
			wantEnd: _endIndicatorLimitedLines,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, resp := tt.build()
			defer releaseReqAndResp(req, resp)
			equal(t, tt.wantEnd, resp.endIndicator)
		})
	}
}

func TestValidCrawlerSelector(t *testing.T) {
	tests := []struct {
		give    string
		wantErr error
	}{
		{give: "all"},
		{give: "hash"},
		{give: "1,3,10"},
		{give: "all\r\nwatch", wantErr: ErrInvalidArgument},
		{give: "1 2", wantErr: ErrInvalidArgument},
		{give: "", wantErr: ErrInvalidArgument},
	}
	for _, tt := range tests {
		name := tt.give
		if name == "" {
			name = "empty"
		}
		t.Run(name, func(t *testing.T) {
			isErrorIs(t, validCrawlerSelector(tt.give), tt.wantErr)
		})
	}
}

func TestValidCrawlClasses(t *testing.T) {
	tests := []struct {
		give    string
		wantErr error
	}{
		{give: "all"},
		{give: "1,3,10"},
		{give: "hash", wantErr: ErrInvalidArgument},
		{give: "", wantErr: ErrInvalidArgument},
	}
	for _, tt := range tests {
		name := tt.give
		if name == "" {
			name = "empty"
		}
		t.Run(name, func(t *testing.T) {
			isErrorIs(t, validCrawlClasses(tt.give), tt.wantErr)
		})
	}
}

func TestMapAdminStatus(t *testing.T) {
	tests := []struct {
		give string
		want error
	}{
		{give: "OK\r\n"},
		{give: "BUSY\r\n", want: ErrBusy},
		{give: "SAME\r\n", want: ErrSame},
		{give: "SAME class\r\n", want: ErrSame},
		{give: "BADCLASS\r\n", want: ErrBadClass},
		{give: "NOSPARE no class\r\n", want: ErrNoSpare},
		{give: "NOTFULL\r\n", want: ErrNotFull},
		{give: "NOTFULL class\r\n", want: ErrNotFull},
		{give: "UNSAFE\r\n", want: ErrUnsafe},
		{give: "UNSAFE class\r\n", want: ErrUnsafe},
		{give: "ERROR\r\n", want: ErrNonexistentCommand},
		{give: "ERROR bad args\r\n", want: ErrCommandError},
		{give: "BUSY crawler\r\n", want: ErrBusy},
		{give: "WATCHER_TOO_MANY log watcher limit reached\r\n", want: ErrWatcherTooMany},
		{give: "WATCHER_FAILED failed to add log watcher\r\n", want: ErrWatcherFailed},
		{give: "MEMLIMIT_TOO_SMALL cannot set maxbytes to less than 8m\r\n", want: ErrMemlimitTooSmall},
		{give: "MEMLIMIT_ADJUST_FAILED out of bounds\r\n", want: ErrMemlimitAdjustFailed},
		{give: "NOTSTARTED no items to crawl\r\n", want: ErrNotStarted},
		{give: "SERVER_ERROR: shutdown not enabled\r\n", want: ErrServerError},
		{give: "ERROR: shutdown not enabled\r\n", want: ErrCommandError},
	}
	for _, tt := range tests {
		t.Run(tt.give, func(t *testing.T) {
			isErrorIs(t, mapAdminStatus([]byte(tt.give)), tt.want)
		})
	}
	err := mapAdminStatus([]byte("BUSY crawler\r\n"))
	contains(t, errText(t, err), "crawler")
}

func TestIsShutdownClosed(t *testing.T) {
	tests := []struct {
		name string
		give error
		want bool
	}{
		{name: "EOF", give: io.EOF, want: true},
		{name: "ErrClosed", give: net.ErrClosed, want: true},
		{name: "ECONNRESET", give: &net.OpError{Err: syscall.ECONNRESET}, want: true},
		{name: "plain reset text", give: errors.New("connection reset by peer")},
		{name: "malformed", give: ErrMalformedResponse},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			equal(t, tt.want, isShutdownClosed(tt.give))
		})
	}
}

func TestAdminStreamLineError(t *testing.T) {
	tests := []struct {
		give string
		want error
	}{
		{give: "key=foo exp=-1"},
		{give: "BUSY", want: ErrBusy},
		{give: "BUSY crawler", want: ErrBusy},
		{give: "ERROR", want: ErrNonexistentCommand},
		{give: "ERROR bad args", want: ErrCommandError},
		{give: "NOSPARE no class", want: ErrNoSpare},
		{give: "NOTFULL class", want: ErrNotFull},
		{give: "UNSAFE class", want: ErrUnsafe},
		{give: "SAME class", want: ErrSame},
		{give: "CLIENT_ERROR bad", want: ErrClientError},
	}
	for _, tt := range tests {
		t.Run(tt.give, func(t *testing.T) {
			isErrorIs(t, adminStreamLineError([]byte(tt.give)), tt.want)
		})
	}
}
