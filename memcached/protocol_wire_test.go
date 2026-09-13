package memcached

import (
	"errors"
	"testing"
)

func TestMapWireLineGlobalAndAdmin(t *testing.T) {
	cases := []struct {
		line    string
		kind    WireKind
		want    error
		discard bool
		end     bool
	}{
		{"ERROR\r\n", WireApp, ErrNonexistentCommand, false, false},
		{"ERROR bad args\r\n", WireApp, ErrCommandError, false, false},
		{"ERROR: shutdown not enabled\r\n", WireApp, ErrCommandError, false, false},
		{"CLIENT_ERROR bad flags\r\n", WireApp, ErrClientError, false, false},
		{"SERVER_ERROR oom\r\n", WireApp, ErrServerError, true, false},
		{"SERVER_ERROR: shutdown not enabled\r\n", WireAdmin, ErrServerError, true, false},
		{"NOT_STORED\r\n", WireApp, ErrNotStored, false, false},
		{"EXISTS\r\n", WireApp, ErrExists, false, false},
		{"NOT_FOUND\r\n", WireApp, ErrNotFound, false, false},
		{"BUSY crawler\r\n", WireAdmin, ErrBusy, false, false},
		{"NOSPARE source class has no spare pages\r\n", WireAdmin, ErrNoSpare, false, false},
		{"WATCHER_TOO_MANY log watcher limit reached\r\n", WireAdmin, ErrWatcherTooMany, false, false},
		{"WATCHER_FAILED failed to add log watcher\r\n", WireAdmin, ErrWatcherFailed, false, false},
		{"MEMLIMIT_TOO_SMALL cannot set maxbytes to less than 8m\r\n", WireAdmin, ErrMemlimit, false, false},
		{"NOTSTARTED no items to crawl\r\n", WireAdmin, ErrNotStarted, false, false},
		{"ERROR bad args\r\n", WireStats, ErrCommandError, false, false},
		{"ERROR bad args\r\n", WireMeta, ErrCommandError, false, false},
		{"EN\r\n", WireDump, nil, false, true},
		{"END\r\n", WireDump, nil, false, true},
		{"XX\r\n", WireMeta, ErrMalformedResponse, true, false},
		{"ST\r\n", WireMeta, ErrMalformedResponse, true, false},
		{"DE\r\n", WireMeta, ErrMalformedResponse, true, false},
	}
	for _, tc := range cases {
		hit := MapWireLine([]byte(tc.line), tc.kind)
		if tc.end {
			equal(t, WireStreamEnd, hit.Class)
			isNil(t, hit.Err)
			continue
		}
		isErrorIs(t, hit.Err, tc.want)
		equal(t, tc.discard, hit.Discard)
	}
}

func TestMapWireLineKeepsMessage(t *testing.T) {
	err := MapWireLine([]byte("CLIENT_ERROR bad flags\r\n"), WireApp).Err
	isErrorIs(t, err, ErrClientError)
	contains(t, err.Error(), "bad flags")
	err = MapWireLine([]byte("SERVER_ERROR oom\r\n"), WireApp).Err
	isErrorIs(t, err, ErrServerError)
	contains(t, err.Error(), "oom")
	isFalse(t, errors.Is(err, ErrTransport))
}

func TestMapWireLineMetaStatus(t *testing.T) {
	equal(t, MetaStatusEN, MapWireLine([]byte("EN kfoo\r\n"), WireMeta).Status)
	equal(t, MetaStatusNF, MapWireLine([]byte("NF\r\n"), WireMeta).Status)
	equal(t, MetaStatusNS, MapWireLine([]byte("NS\r\n"), WireMeta).Status)
	equal(t, MetaStatusEX, MapWireLine([]byte("EX\r\n"), WireMeta).Status)
	equal(t, MetaStatusHD, MapWireLine([]byte("HD\r\n"), WireMeta).Status)
	isErrorIs(t, MapWireLine([]byte("EN\r\n"), WireMeta).Err, ErrNotFound)
	equal(t, error(nil), MapWireLine([]byte("END\r\n"), WireMeta).Err)
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
	_, err := parseStatsMap([][]byte{[]byte("ERROR bad args\r\n"), []byte("END\r\n")})
	isErrorIs(t, err, ErrCommandError)
}

func TestAdminKeywordKeepsMessage(t *testing.T) {
	err := mapAdminStatus([]byte("BUSY crawler\r\n"))
	isErrorIs(t, err, ErrBusy)
	contains(t, err.Error(), "crawler")
}

func TestIsDumpEnd(t *testing.T) {
	isTrue(t, isDumpEnd("EN"))
	isTrue(t, isDumpEnd("END"))
	isFalse(t, isDumpEnd("ENTRY"))
	isFalse(t, isDumpEnd("ERROR"))
}

func TestMetaStatusErr(t *testing.T) {
	isErrorIs(t, MetaStatusEN.Err(), ErrNotFound)
	isErrorIs(t, MetaStatusNF.Err(), ErrNotFound)
	isErrorIs(t, MetaStatusNS.Err(), ErrNotStored)
	isErrorIs(t, MetaStatusEX.Err(), ErrExists)
	isNil(t, MetaStatusHD.Err())
	isErrorIs(t, MetaResult{Status: MetaStatusEN}.Err(), ErrNotFound)
}
