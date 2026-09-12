package memcached

import (
	"errors"
	"testing"
	"time"
)

func TestEncodeGetOneCommand(t *testing.T) {
	req, err := encodeGetOneCommand("get", 0, []byte("k"), nil)
	req = mustBuildReq(t, req, err)
	equal(t, []byte("get k\r\n"), req.raw)
	req, err = encodeGetOneCommand("gats", Rel(time.Second), []byte("k"), nil)
	req = mustBuildReq(t, req, err)
	equal(t, []byte("gats 1 k\r\n"), req.raw)
}

func TestEncodeGetAndTouchesCommand(t *testing.T) {
	req, err := encodeGetAndTouchesCommand("gats", Rel(time.Second), nil, []byte("key1"), []byte("key2"))
	req = mustBuildReq(t, req, err)
	defer req.release()
	equal(t, []byte("gats 1 key1 key2\r\n"), req.raw)
}

func TestParseUintFromBytes(t *testing.T) {
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

func TestParseValueLine(t *testing.T) {
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
			name: "normal case without cas",
			line: []byte("VALUE mykey 123 456"),
			wantItem: &Item{
				Key:   []byte("mykey"),
				Flags: 123,
			},
			wantLen: 456,
		},
		{
			name:    "normal case with cas",
			line:    []byte("VALUE mykey 123 456 789"),
			withCas: true,
			wantItem: &Item{
				Key:   []byte("mykey"),
				Flags: 123,
				CAS:   789,
			},
			wantLen: 456,
		},
		{
			name:       "invalid flags",
			line:       []byte("VALUE mykey abc 456"),
			wantItem:   &Item{Key: []byte("mykey")},
			wantErr:    true,
			wantErrMsg: "invalid flags",
		},
		{
			name:       "invalid data length",
			line:       []byte("VALUE mykey 123 abc "),
			wantItem:   &Item{Key: []byte("mykey"), Flags: 123},
			wantErr:    true,
			wantErrMsg: "invalid data length",
		},
		{
			name:       "too many fields without cas",
			line:       []byte("VALUE mykey 123 456 789 extra "),
			wantErr:    true,
			wantErrMsg: "invalid VALUE line",
		},
		{
			name:       "too many fields with cas",
			line:       []byte("VALUE mykey 123 456 789 extra more"),
			withCas:    true,
			wantErr:    true,
			wantErrMsg: "invalid VALUE line",
		},
		{
			name:       "empty line",
			line:       []byte("VALUE "),
			wantErr:    true,
			wantErrMsg: "empty VALUE key",
		},
		{
			name:       "missing CAS when required",
			line:       []byte("VALUE mykey 123 456"),
			withCas:    true,
			wantItem:   &Item{Key: []byte("mykey"), Flags: 123},
			wantLen:    456,
			wantErr:    true,
			wantErrMsg: "missing CAS field",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := &Item{}
			gotLen, err := parseValueLine(tt.line, item, tt.withCas)
			if tt.wantErr {
				isError(t, err)
				isTrue(t, errors.Is(err, ErrMalformedResponse))
				contains(t, errText(t, err), tt.wantErrMsg)
				return
			}
			noError(t, err)
			equal(t, tt.wantLen, gotLen)
			equal(t, tt.wantItem.Key, item.Key)
			equal(t, tt.wantItem.Flags, item.Flags)
			equal(t, tt.wantItem.CAS, item.CAS)
		})
	}
}

func TestBuildGetsCommandSetsPickKey(t *testing.T) {
	req, resp, err := buildGetsCommand("get", []byte("only"))
	req, resp = mustBuildPair(t, req, resp, err)
	defer releaseReqAndResp(req, resp)
	equal(t, []byte("only"), req.key)

	req, resp, err = buildGetsCommand("get", []byte("a"), []byte("b"))
	req, resp = mustBuildPair(t, req, resp, err)
	defer releaseReqAndResp(req, resp)
	isNil(t, req.key)
}

func TestEncodeGetAndTouchesCommandSetsPickKey(t *testing.T) {
	req, err := encodeGetAndTouchesCommand("gat", Rel(time.Second), nil, []byte("only"))
	req = mustBuildReq(t, req, err)
	defer req.release()
	equal(t, []byte("only"), req.key)

	req, err = encodeGetAndTouchesCommand("gat", Rel(time.Second), nil, []byte("a"), []byte("b"))
	req = mustBuildReq(t, req, err)
	defer req.release()
	isNil(t, req.key)
}
