package memcached

import (
	"errors"
	"strings"
	"testing"
)

func TestMapWireLineGlobalAndAdmin(t *testing.T) {
	tests := []struct {
		line    string
		kind    wireKind
		want    error
		discard bool
	}{
		{line: "ERROR\r\n", kind: _wireApp, want: ErrNonexistentCommand},
		{line: "ERROR bad args\r\n", kind: _wireApp, want: ErrCommandError},
		{line: "ERROR: shutdown not enabled\r\n", kind: _wireApp, want: ErrCommandError},
		{line: "CLIENT_ERROR bad flags\r\n", kind: _wireApp, want: ErrClientError},
		{line: "SERVER_ERROR oom\r\n", kind: _wireApp, want: ErrServerError, discard: true},
		{line: "SERVER_ERROR: shutdown not enabled\r\n", kind: _wireAdmin, want: ErrServerError, discard: true},
		{line: "NOT_STORED\r\n", kind: _wireApp, want: ErrNotStored},
		{line: "EXISTS\r\n", kind: _wireApp, want: ErrExists},
		{line: "NOT_FOUND\r\n", kind: _wireApp, want: ErrNotFound},
		{line: "BUSY crawler\r\n", kind: _wireAdmin, want: ErrBusy},
		{line: "NOSPARE source class has no spare pages\r\n", kind: _wireAdmin, want: ErrNoSpare},
		{line: "WATCHER_TOO_MANY log watcher limit reached\r\n", kind: _wireAdmin, want: ErrWatcherTooMany},
		{line: "WATCHER_FAILED failed to add log watcher\r\n", kind: _wireAdmin, want: ErrWatcherFailed},
		{line: "MEMLIMIT_TOO_SMALL cannot set maxbytes to less than 8m\r\n", kind: _wireAdmin, want: ErrMemlimitTooSmall},
		{line: "MEMLIMIT_ADJUST_FAILED out of bounds\r\n", kind: _wireAdmin, want: ErrMemlimitAdjustFailed},
		{line: "PREFIX name get 1 hit 0 set 0 del 0\r\n", kind: _wireStats},
		{line: "OK\r\n", kind: _wireStats},
		{line: "END\r\n", kind: _wireMeta, want: ErrMalformedResponse, discard: true},
		{line: "ENTRY\r\n", kind: _wireMeta, want: ErrMalformedResponse, discard: true},
		{line: "NOTSTARTED no items to crawl\r\n", kind: _wireAdmin, want: ErrNotStarted},
		{line: "ERROR bad args\r\n", kind: _wireStats, want: ErrCommandError},
		{line: "ERROR bad args\r\n", kind: _wireMeta, want: ErrCommandError},
		{line: "XX\r\n", kind: _wireMeta, want: ErrMalformedResponse, discard: true},
		{line: "ST\r\n", kind: _wireMeta, want: ErrMalformedResponse, discard: true},
	}
	for _, tt := range tests {
		t.Run(strings.TrimSuffix(tt.line, "\r\n"), func(t *testing.T) {
			hit := mapWireLine([]byte(tt.line), tt.kind)
			isErrorIs(t, hit.Err, tt.want)
			equal(t, tt.discard, hit.Discard)
		})
	}
}

func TestMapWireLineDumpEnd(t *testing.T) {
	tests := []struct {
		line string
	}{
		{line: "EN\r\n"},
		{line: "END\r\n"},
	}
	for _, tt := range tests {
		t.Run(strings.TrimSuffix(tt.line, "\r\n"), func(t *testing.T) {
			hit := mapWireLine([]byte(tt.line), _wireDump)
			equal(t, _wireStreamEnd, hit.Class)
			isNil(t, hit.Err)
		})
	}
}

func TestMapWireLineKeepsMessage(t *testing.T) {
	err := mapWireLine([]byte("CLIENT_ERROR bad flags\r\n"), _wireApp).Err
	isErrorIs(t, err, ErrClientError)
	contains(t, errText(t, err), "bad flags")
	err = mapWireLine([]byte("SERVER_ERROR oom\r\n"), _wireApp).Err
	isErrorIs(t, err, ErrServerError)
	contains(t, errText(t, err), "oom")
	isFalse(t, errors.Is(err, ErrTransport))
}

func TestMapWireLineMetaStatus(t *testing.T) {
	tests := []struct {
		line string
		want MetaStatus
	}{
		{line: "EN kfoo\r\n", want: MetaStatusEN},
		{line: "NF\r\n", want: MetaStatusNF},
		{line: "NS\r\n", want: MetaStatusNS},
		{line: "EX\r\n", want: MetaStatusEX},
		{line: "HD\r\n", want: MetaStatusHD},
		{line: "DE\r\n", want: MetaStatusHD}, // quiet md alias
	}
	for _, tt := range tests {
		t.Run(strings.TrimSuffix(tt.line, "\r\n"), func(t *testing.T) {
			equal(t, tt.want, mapWireLine([]byte(tt.line), _wireMeta).Status)
		})
	}
}

func TestMapWireLineMetaErrors(t *testing.T) {
	tests := []struct {
		line    string
		want    error
		discard bool
	}{
		{line: "EN\r\n", want: ErrNotFound},
		{line: "END\r\n", want: ErrMalformedResponse, discard: true},
		{line: "ENTRY\r\n", want: ErrMalformedResponse, discard: true},
	}
	for _, tt := range tests {
		t.Run(strings.TrimSuffix(tt.line, "\r\n"), func(t *testing.T) {
			hit := mapWireLine([]byte(tt.line), _wireMeta)
			isErrorIs(t, hit.Err, tt.want)
			equal(t, tt.discard, hit.Discard)
		})
	}
}

func TestForecastKeepsUnknownAppLines(t *testing.T) {
	equal(t, error(nil), forecastCommonFaultLine([]byte("VALUE k 0 3\r\n")))
	equal(t, error(nil), forecastCommonFaultLine([]byte("HD\r\n")))
	isErrorIs(t, forecastCommonFaultLine([]byte("ERROR bad args\r\n")), ErrCommandError)
}

func TestParseMetaItemRejectsUnknownCD(t *testing.T) {
	isErrorIs(t, parseMetaItem([][]byte{[]byte("XX\r\n")}, &MetaItem{}, false, false), ErrMalformedResponse)
	isErrorIs(t, parseMetaItem([][]byte{[]byte("ST\r\n")}, &MetaItem{}, false, false), ErrMalformedResponse)
}

func TestParseStatsMapCommandError(t *testing.T) {
	var got []Stat
	err := parseStatsInto([][]byte{[]byte("ERROR bad args\r\n"), []byte("END\r\n")}, &got)
	isErrorIs(t, err, ErrCommandError)
}

func TestAdminKeywordKeepsMessage(t *testing.T) {
	err := mapAdminStatus([]byte("BUSY crawler\r\n"))
	isErrorIs(t, err, ErrBusy)
	contains(t, errText(t, err), "crawler")
}

func TestIsDumpEnd(t *testing.T) {
	tests := []struct {
		give string
		want bool
	}{
		{give: "EN", want: true},
		{give: "END", want: true},
		{give: "ENTRY"},
		{give: "ERROR"},
	}
	for _, tt := range tests {
		t.Run(tt.give, func(t *testing.T) {
			equal(t, tt.want, isDumpEnd(tt.give))
		})
	}
}

func TestMetaStatusErr(t *testing.T) {
	tests := []struct {
		name string
		got  error
		want error
	}{
		{name: "EN", got: MetaStatusEN.Err(), want: ErrNotFound},
		{name: "NF", got: MetaStatusNF.Err(), want: ErrNotFound},
		{name: "NS", got: MetaStatusNS.Err(), want: ErrNotStored},
		{name: "EX", got: MetaStatusEX.Err(), want: ErrExists},
		{name: "HD", got: MetaStatusHD.Err()},
		{name: "result EN", got: MetaResult{Status: MetaStatusEN}.Err(), want: ErrNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isErrorIs(t, tt.got, tt.want)
		})
	}
}
