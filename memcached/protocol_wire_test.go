package memcached

import (
	"errors"
	"testing"
)

func Test_mapWireLineGlobalAndAdmin(t *testing.T) {
	cases := []struct {
		line    string
		kind    wireKind
		want    error
		discard bool
		end     bool
	}{
		{"ERROR\r\n", wireApp, ErrNonexistentCommand, false, false},
		{"ERROR bad args\r\n", wireApp, ErrCommandError, false, false},
		{"ERROR: shutdown not enabled\r\n", wireApp, ErrCommandError, false, false},
		{"CLIENT_ERROR bad flags\r\n", wireApp, ErrClientError, false, false},
		{"SERVER_ERROR oom\r\n", wireApp, ErrServerError, true, false},
		{"SERVER_ERROR: shutdown not enabled\r\n", wireAdmin, ErrServerError, true, false},
		{"NOT_STORED\r\n", wireApp, ErrNotStored, false, false},
		{"EXISTS\r\n", wireApp, ErrExists, false, false},
		{"NOT_FOUND\r\n", wireApp, ErrNotFound, false, false},
		{"BUSY crawler\r\n", wireAdmin, ErrBusy, false, false},
		{"NOSPARE source class has no spare pages\r\n", wireAdmin, ErrNoSpare, false, false},
		{"WATCHER_TOO_MANY log watcher limit reached\r\n", wireAdmin, ErrWatcherTooMany, false, false},
		{"WATCHER_FAILED failed to add log watcher\r\n", wireAdmin, ErrWatcherFailed, false, false},
		{"MEMLIMIT_TOO_SMALL cannot set maxbytes to less than 8m\r\n", wireAdmin, ErrMemlimitTooSmall, false, false},
		{"MEMLIMIT_ADJUST_FAILED out of bounds\r\n", wireAdmin, ErrMemlimitAdjustFailed, false, false},
		{"PREFIX name get 1 hit 0 set 0 del 0\r\n", wireStats, nil, false, false},
		{"OK\r\n", wireStats, nil, false, false},
		{"END\r\n", wireMeta, ErrMalformedResponse, true, false},
		{"ENTRY\r\n", wireMeta, ErrMalformedResponse, true, false},
		{"NOTSTARTED no items to crawl\r\n", wireAdmin, ErrNotStarted, false, false},
		{"ERROR bad args\r\n", wireStats, ErrCommandError, false, false},
		{"ERROR bad args\r\n", wireMeta, ErrCommandError, false, false},
		{"EN\r\n", wireDump, nil, false, true},
		{"END\r\n", wireDump, nil, false, true},
		{"XX\r\n", wireMeta, ErrMalformedResponse, true, false},
		{"ST\r\n", wireMeta, ErrMalformedResponse, true, false},
		{"DE\r\n", wireMeta, ErrMalformedResponse, true, false},
	}
	for _, tc := range cases {
		hit := mapWireLine([]byte(tc.line), tc.kind)
		if tc.end {
			equal(t, wireStreamEnd, hit.Class)
			isNil(t, hit.Err)
			continue
		}
		isErrorIs(t, hit.Err, tc.want)
		equal(t, tc.discard, hit.Discard)
	}
}

func Test_mapWireLineKeepsMessage(t *testing.T) {
	err := mapWireLine([]byte("CLIENT_ERROR bad flags\r\n"), wireApp).Err
	isErrorIs(t, err, ErrClientError)
	contains(t, err.Error(), "bad flags")
	err = mapWireLine([]byte("SERVER_ERROR oom\r\n"), wireApp).Err
	isErrorIs(t, err, ErrServerError)
	contains(t, err.Error(), "oom")
	isFalse(t, errors.Is(err, ErrTransport))
}

func Test_mapWireLineMetaStatus(t *testing.T) {
	equal(t, MetaStatusEN, mapWireLine([]byte("EN kfoo\r\n"), wireMeta).Status)
	equal(t, MetaStatusNF, mapWireLine([]byte("NF\r\n"), wireMeta).Status)
	equal(t, MetaStatusNS, mapWireLine([]byte("NS\r\n"), wireMeta).Status)
	equal(t, MetaStatusEX, mapWireLine([]byte("EX\r\n"), wireMeta).Status)
	equal(t, MetaStatusHD, mapWireLine([]byte("HD\r\n"), wireMeta).Status)
	isErrorIs(t, mapWireLine([]byte("EN\r\n"), wireMeta).Err, ErrNotFound)
	isErrorIs(t, mapWireLine([]byte("END\r\n"), wireMeta).Err, ErrMalformedResponse)
	isErrorIs(t, mapWireLine([]byte("ENTRY\r\n"), wireMeta).Err, ErrMalformedResponse)
	isTrue(t, mapWireLine([]byte("END\r\n"), wireMeta).Discard)
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
