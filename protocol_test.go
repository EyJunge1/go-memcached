package memcached

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func Test_parseValueItems(t *testing.T) {
	type args struct {
		lines          [][]byte
		withoutEndLine bool
		withCAS        bool
	}
	tests := []struct {
		name    string
		args    args
		want    []*Item
		wantErr bool
	}{
		{
			name: "normal",
			args: args{
				lines: [][]byte{
					[]byte("VALUE key 0 5\r\n"),
					[]byte("value\r\n"),
					[]byte("VALUE key2 0 6\r\n"),
					[]byte("value2\r\n"),
					[]byte("END\r\n"),
				},
				withoutEndLine: false,
				withCAS:        false,
			},
			want: []*Item{
				{
					Key:   "key",
					Value: []byte("value"),
				},
				{
					Key:   "key2",
					Value: []byte("value2"),
				},
			},
			wantErr: false,
		},
		{
			name: "with flags/cas-unique",
			args: args{
				lines: [][]byte{
					[]byte("VALUE key 123 5 1\r\n"),
					[]byte("value\r\n"),
					[]byte("VALUE key2 123 6 2\r\n"),
					[]byte("value2\r\n"),
					[]byte("END\r\n"),
				},
				withoutEndLine: false,
				withCAS:        true,
			},
			want: []*Item{
				{
					Key:   "key",
					Value: []byte("value"),
					Flags: 123,
					CAS:   1,
				},
				{
					Key:   "key2",
					Value: []byte("value2"),
					Flags: 123,
					CAS:   2,
				},
			},
			wantErr: false,
		},
		{
			name: "without end line",
			args: args{
				lines: [][]byte{
					[]byte("VALUE key 123 5 1\r\n"),
					[]byte("value\r\n"),
					[]byte("VALUE key2 123 6 2\r\n"),
					[]byte("value2\r\n"),
				},
				withoutEndLine: true,
				withCAS:        true,
			},
			want: []*Item{
				{
					Key:   "key",
					Value: []byte("value"),
					Flags: 123,
					CAS:   1,
				},
				{
					Key:   "key2",
					Value: []byte("value2"),
					Flags: 123,
					CAS:   2,
				},
			},
			wantErr: false,
		},
		{
			name: "malformed-1",
			args: args{
				lines: [][]byte{
					[]byte("VALUE key -1 5\r\n"),
					[]byte("value\r\n"),
					[]byte("END\r\n"),
				},
				withoutEndLine: false,
				withCAS:        false,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "malformed-2",
			args: args{
				lines: [][]byte{
					[]byte("VALUE key 0 5\r\n"),
				},
				withoutEndLine: true,
			},
			want:    nil,
			wantErr: true, // missing data block
		},
		{
			name: "rejects non-VALUE protocol line",
			args: args{
				lines: [][]byte{
					[]byte("HD\r\n"),
					[]byte("ignored\r\n"),
					[]byte("END\r\n"),
				},
				withoutEndLine: false,
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseValueItems(tt.args.lines, tt.args.withoutEndLine, tt.args.withCAS)
			if tt.wantErr {
				isError(t, err)
				return
			}

			noError(t, err)
			equal(t, tt.want, got)
		})
	}
}

func Test_parseValueItemsPreservesOpaqueValue(t *testing.T) {
	value := []byte{0x1f, 0x8b, 0x08, 0x00}

	lines := [][]byte{
		[]byte("VALUE key 18 4\r\n"),
		append(append([]byte{}, value...), []byte("\r\n")...),
		[]byte("END\r\n"),
	}

	items, err := parseValueItems(lines, false, false)
	noError(t, err)
	hasLen(t, items, 1)
	equal(t, value, items[0].Value)
	equal(t, uint32(18), items[0].Flags)
}

func Test_buildGetAndTouchesCommand(t *testing.T) {
	req, resp := buildGetAndTouchesCommand("gats", time.Second, "key1", "key2")
	defer releaseReqAndResp(req, resp)

	equal(t, []byte("gats 1 key1 key2\r\n"), req.raw)
	equal(t, endIndicatorSpecificEndLine, resp.endIndicator)
	equal(t, _EndCRLFBytes, resp.specEndLine)
	hasLen(t, resp.rawLines, 0)
}

func constructParts(raw []byte) [][]byte {
	return bytes.Split(trimCRLF(raw), []byte(" "))
}

func Test_parseUintFromBytes(t *testing.T) {
	type args struct {
		raw []byte
	}
	tests := []struct {
		name    string
		args    args
		want    uint64
		wantErr bool
	}{
		{
			name: "normal-1",
			args: args{
				raw: []byte("123"),
			},
			want:    123,
			wantErr: false,
		},
		{
			name: "normal-2",
			args: args{
				raw: []byte("1234567890"),
			},
			want:    1234567890,
			wantErr: false,
		},
		{
			name: "max-uint64",
			args: args{
				raw: []byte("18446744073709551615"),
			},
			want:    ^uint64(0),
			wantErr: false,
		},
		{
			name: "overflow-uint64",
			args: args{
				raw: []byte("18446744073709551616"),
			},
			want:    0,
			wantErr: true,
		},
		{
			name: "overflow-long-number",
			args: args{
				raw: []byte("9999999999999999999999999999999999999999"),
			},
			want:    0,
			wantErr: true,
		},
		{
			name: "malformed-contains-letters",
			args: args{
				raw: []byte("abc"),
			},
			want:    0,
			wantErr: true,
		},
		{
			name: "malformed-contains-other-chars",
			args: args{
				raw: []byte("1234567890abc"),
			},
			want:    0,
			wantErr: true,
		},
		{
			name: "malformed-empty",
			args: args{
				raw: []byte(""),
			},
			want:    0,
			wantErr: false,
		},
		{
			name: "malformed-space",
			args: args{
				raw: []byte(" "),
			},
			want:    0,
			wantErr: true,
		},
		{
			name: "malformed-negative",
			args: args{
				raw: []byte("-123"),
			},
			want:    0,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseUintFromBytes(tt.args.raw)
			if tt.wantErr {
				isError(t, err)
				return
			}
			noError(t, err)
			equal(t, tt.want, got)
		})
	}
}

func Test_parseValueLine(t *testing.T) {
	tests := []struct {
		name       string
		line       []byte
		withCas    bool
		wantItem   *Item
		wantLen    uint64
		wantErr    bool
		wantErrMsg string
	}{
		{
			name:    "normal case without cas",
			line:    []byte("VALUE mykey 123 456"),
			withCas: false,
			wantItem: &Item{
				Key:   "mykey",
				Flags: 123,
				CAS:   0,
			},
			wantLen: 456,
			wantErr: false,
		},
		{
			name:    "normal case with cas",
			line:    []byte("VALUE mykey 123 456 789"),
			withCas: true,
			wantItem: &Item{
				Key:   "mykey",
				Flags: 123,
				CAS:   789,
			},
			wantLen: 456,
			wantErr: false,
		},
		{
			name:       "invalid flags",
			line:       []byte("VALUE mykey abc 456"),
			withCas:    false,
			wantItem:   &Item{Key: "mykey"},
			wantLen:    0,
			wantErr:    true,
			wantErrMsg: "invalid flags",
		},
		{
			name:       "invalid data length",
			line:       []byte("VALUE mykey 123 abc "),
			withCas:    false,
			wantItem:   &Item{Key: "mykey", Flags: 123},
			wantLen:    0,
			wantErr:    true,
			wantErrMsg: "invalid data length",
		},
		{
			name:       "too many fields without cas",
			line:       []byte("VALUE mykey 123 456 789 extra "),
			withCas:    false,
			wantItem:   &Item{},
			wantLen:    0,
			wantErr:    true,
			wantErrMsg: "invalid VALUE line",
		},
		{
			name:       "too many fields with cas",
			line:       []byte("VALUE mykey 123 456 789 extra more"),
			withCas:    true,
			wantItem:   &Item{},
			wantLen:    0,
			wantErr:    true,
			wantErrMsg: "invalid VALUE line",
		},
		{
			name:       "empty line",
			line:       []byte("VALUE "),
			withCas:    false,
			wantItem:   &Item{},
			wantLen:    0,
			wantErr:    true,
			wantErrMsg: "empty VALUE key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := &Item{}
			gotLen, err := parseValueLine(tt.line, item, tt.withCas)

			if tt.wantErr {
				isError(t, err)
				isTrue(t, errors.Is(err, ErrMalformedResponse))
				contains(t, err.Error(), tt.wantErrMsg)
			} else {
				noError(t, err)
				equal(t, tt.wantLen, gotLen)
				equal(t, tt.wantItem.Key, item.Key)
				equal(t, tt.wantItem.Flags, item.Flags)
				equal(t, tt.wantItem.CAS, item.CAS)
			}
		})
	}
}

func Test_parseStats(t *testing.T) {
	type args struct {
		lines [][]byte
	}
	tests := []struct {
		name    string
		args    args
		want    *Statistic
		wantErr bool
	}{
		{
			name: "normal case",
			args: args{
				lines: [][]byte{
					[]byte("STAT version 1.5.12"),
					[]byte("STAT pid 12345"),
					[]byte("STAT uptime 123456"),
					[]byte("STAT time 1234567890"),
					[]byte("STAT pointer_size 64"),
					[]byte("STAT rusage_user 30440.595477"),
					[]byte("STAT rusage_system 41317.488860"),
					[]byte("STAT curr_connections 123"),
					[]byte("STAT total_connections 123456"),
					[]byte("STAT connection_structures 1234567890"),
					[]byte("STAT reserved_fds 1234567890"),
					[]byte("STAT accepting_conns 1"),
					[]byte("STAT hash_is_expanding 1"),
					[]byte("STAT custom_ext_key 99"),
				},
			},
			want: &Statistic{
				Version:              "1.5.12",
				PID:                  12345,
				Uptime:               123456,
				Time:                 1234567890,
				PointerSize:          64,
				RusageUser:           30440.595477,
				RusageSystem:         41317.488860,
				CurrConnections:      123,
				TotalConnections:     123456,
				ConnectionStructures: 1234567890,
				ReservedFDs:          1234567890,
				AcceptingConns:       true,
				HashIsExpanding:      true,
				Extra:                map[string]string{"custom_ext_key": "99"},
			},
		},
		{
			name: "END only",
			args: args{
				lines: [][]byte{[]byte("END\r\n")},
			},
			want: &Statistic{},
		},
		{
			name: "unparseable known key goes to Extra",
			args: args{
				lines: [][]byte{
					[]byte("STAT pid not-a-number"),
					[]byte("STAT version 1.6.0 with build info"),
				},
			},
			want: &Statistic{
				Version: "1.6.0 with build info",
				Extra:   map[string]string{"pid": "not-a-number"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseStats(tt.args.lines)
			if tt.wantErr {
				isError(t, err)
				return
			}
			noError(t, err)
			equal(t, tt.want, got)
		})
	}
}

func Test_validateKeyAndValue(t *testing.T) {
	noError(t, validateKeyAndValue([]byte("key"), []byte("value")))
	noError(t, validateKeyAndValue([]byte("key"), nil))
	isErrorIs(t, validateKeyAndValue(nil, []byte("value")), ErrInvalidKey)
	isErrorIs(t, validateKeyAndValue([]byte(""), []byte("value")), ErrInvalidKey)
	isErrorIs(t, validateKeyAndValue(bytes.Repeat([]byte("a"), maxKeySize+1), nil), ErrInvalidKey)
	isErrorIs(t, validateKeyAndValue([]byte("has space"), nil), ErrInvalidKey)
	isErrorIs(t, validateKeyAndValue([]byte("has\ttab"), nil), ErrInvalidKey)
	isErrorIs(t, validateKeyAndValue([]byte{0x7f}, nil), ErrInvalidKey)
	noError(t, validateKeyAndValueFlags([]byte("has space"), nil, true))
	isErrorIs(t, validateKeyAndValueFlags(bytes.Repeat([]byte("a"), 188), nil, true), ErrInvalidKey)
	noError(t, validateKeyAndValueFlags(bytes.Repeat([]byte("a"), 186), nil, true))
	equal(t, 250, maxKeySize)
}

func Test_buildFlushAllCommand_delay(t *testing.T) {
	req, resp := buildFlushAllCommand(0, false)
	defer releaseReqAndResp(req, resp)
	equal(t, []byte("flush_all\r\n"), req.raw)

	req, resp = buildFlushAllCommand(10*time.Second, false)
	defer releaseReqAndResp(req, resp)
	equal(t, []byte("flush_all 10\r\n"), req.raw)
}

func Test_parseStatsMap(t *testing.T) {
	got, err := parseStatsMap([][]byte{
		[]byte("STAT items:1:number 2\r\n"),
		[]byte("END\r\n"),
	})
	noError(t, err)
	equal(t, "2", got["items:1:number"])
}

func Test_expiryToken(t *testing.T) {
	equal(t, int64(0), expiryToken(0))
	equal(t, int64(-1), expiryToken(-time.Second))
	equal(t, int64(1), expiryToken(500*time.Millisecond))
	equal(t, int64(1), expiryToken(time.Second))
	equal(t, int64(60), expiryToken(60*time.Second))

	fixed := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	prev := nowFunc
	nowFunc = func() time.Time { return fixed }
	t.Cleanup(func() { nowFunc = prev })
	want := fixed.Add(31 * 24 * time.Hour).Unix()
	equal(t, want, expiryToken(31*24*time.Hour))
}

func Test_buildStorageCommand_expiryWire(t *testing.T) {
	req, resp := buildStorageCommand("set", "k", []byte("v"), 0, 500*time.Millisecond, false)
	defer releaseReqAndResp(req, resp)
	equal(t, []byte("set k 0 1 1\r\nv\r\n"), req.raw)

	req, resp = buildStorageCommand("set", "k", []byte("v"), 0, -time.Second, false)
	defer releaseReqAndResp(req, resp)
	equal(t, []byte("set k 0 -1 1\r\nv\r\n"), req.raw)
}

func Test_buildGetsCommand_setsPickKey(t *testing.T) {
	req, resp := buildGetsCommand("get", "only")
	defer releaseReqAndResp(req, resp)
	equal(t, []byte("only"), req.key)

	req, resp = buildGetsCommand("get", "a", "b")
	defer releaseReqAndResp(req, resp)
	isNil(t, req.key)
}

func Test_buildGetAndTouchesCommand_setsPickKey(t *testing.T) {
	req, resp := buildGetAndTouchesCommand("gat", time.Second, "only")
	defer releaseReqAndResp(req, resp)
	equal(t, []byte("only"), req.key)

	req, resp = buildGetAndTouchesCommand("gat", time.Second, "a", "b")
	defer releaseReqAndResp(req, resp)
	isNil(t, req.key)
}

func Test_mapStorageReply(t *testing.T) {
	stored := &response{endIndicator: endIndicatorLimitedLines, rawLines: [][]byte{_StoredCRLFBytes}}
	noError(t, mapStorageReply(stored))

	notStored := &response{endIndicator: endIndicatorLimitedLines, rawLines: [][]byte{[]byte("NOT_STORED\r\n")}}
	isErrorIs(t, mapStorageReply(notStored), ErrNotStored)
	isFalse(t, shouldDiscardConn(wrapRecvError(mapStorageReply(notStored))))

	exists := &response{endIndicator: endIndicatorLimitedLines, rawLines: [][]byte{[]byte("EXISTS\r\n")}}
	isErrorIs(t, mapStorageReply(exists), ErrExists)
	isFalse(t, shouldDiscardConn(wrapRecvError(mapStorageReply(exists))))

	notFound := &response{endIndicator: endIndicatorLimitedLines, rawLines: [][]byte{[]byte("NOT_FOUND\r\n")}}
	isErrorIs(t, mapStorageReply(notFound), ErrNotFound)

	junk := &response{endIndicator: endIndicatorLimitedLines, rawLines: [][]byte{[]byte("WHATEVER\r\n")}}
	isErrorIs(t, mapStorageReply(junk), ErrMalformedResponse)
	isTrue(t, shouldDiscardConn(wrapRecvError(mapStorageReply(junk))))

	quiet := &response{endIndicator: endIndicatorNoReply}
	noError(t, mapStorageReply(quiet))
}

func Test_dataBlockValueUsesDeclaredLength(t *testing.T) {
	// Value that itself ends in CRLF must not be trimmed down to empty.
	raw := []byte("ab\r\n")
	got, err := dataBlockValue(raw, 2)
	noError(t, err)
	equal(t, []byte("ab"), got)

	valueCRLF := []byte("\r\n\r\n")
	got, err = dataBlockValue(valueCRLF, 2)
	noError(t, err)
	equal(t, []byte("\r\n"), got)
}

func Test_forecastCommonFaultLine(t *testing.T) {
	isErrorIs(t, forecastCommonFaultLine([]byte("NOT_FOUND\r\n")), ErrNotFound)
	isErrorIs(t, forecastCommonFaultLine([]byte("EXISTS\r\n")), ErrExists)
	isErrorIs(t, forecastCommonFaultLine([]byte("NOT_STORED\r\n")), ErrNotStored)
	isErrorIs(t, forecastCommonFaultLine([]byte("EN\r\n")), ErrNotFound)
	isErrorIs(t, forecastCommonFaultLine([]byte("EN kfoo\r\n")), ErrNotFound)
	isErrorIs(t, forecastCommonFaultLine([]byte("ERROR\r\n")), ErrNonexistentCommand)
	equal(t, error(nil), forecastCommonFaultLine([]byte("END\r\n")))
	equal(t, error(nil), forecastCommonFaultLine([]byte("ENTRY\r\n")))
	equal(t, error(nil), forecastCommonFaultLine([]byte("VALUE k 0 3\r\n")))
	isErrorIs(t, forecastCommonFaultLine([]byte("CLIENT_ERROR bad flags\r\n")), ErrClientError)
	isErrorIs(t, forecastCommonFaultLine([]byte("CLIENT_ERROR")), ErrClientError)
	isErrorIs(t, forecastCommonFaultLine([]byte("CLIENT_ERROR\r\n")), ErrClientError)
	isErrorIs(t, forecastCommonFaultLine([]byte("CLIENT_ERROR\n")), ErrClientError)
	isErrorIs(t, forecastCommonFaultLine([]byte("NOT_STORED\n")), ErrNotStored)
	isErrorIs(t, forecastCommonFaultLine([]byte("SERVER_ERROR oom")), ErrServerError)
	equal(t, error(nil), forecastCommonFaultLine([]byte("CLIENT_ERRORFOO\r\n")))
}
