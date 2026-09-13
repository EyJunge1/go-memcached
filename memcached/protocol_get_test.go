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
		want    []Item
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
			want: []Item{
				{
					Key:   []byte("key"),
					Value: []byte("value"),
				},
				{
					Key:   []byte("key2"),
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
			want: []Item{
				{
					Key:   []byte("key"),
					Value: []byte("value"),
					Flags: 123,
					CAS:   1,
				},
				{
					Key:   []byte("key2"),
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
			want: []Item{
				{
					Key:   []byte("key"),
					Value: []byte("value"),
					Flags: 123,
					CAS:   1,
				},
				{
					Key:   []byte("key2"),
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
			wantErr: true,
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
	req, resp := buildGetAndTouchesCommand("gats", Rel(time.Second), []byte("key1"), []byte("key2"))
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
				Key:   []byte("mykey"),
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
				Key:   []byte("mykey"),
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
			wantItem:   &Item{Key: []byte("mykey")},
			wantLen:    0,
			wantErr:    true,
			wantErrMsg: "invalid flags",
		},
		{
			name:       "invalid data length",
			line:       []byte("VALUE mykey 123 abc "),
			withCas:    false,
			wantItem:   &Item{Key: []byte("mykey"), Flags: 123},
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

func Test_buildGetsCommand_setsPickKey(t *testing.T) {
	req, resp := buildGetsCommand("get", []byte("only"))
	defer releaseReqAndResp(req, resp)
	equal(t, []byte("only"), req.key)

	req, resp = buildGetsCommand("get", []byte("a"), []byte("b"))
	defer releaseReqAndResp(req, resp)
	isNil(t, req.key)
}

func Test_buildGetAndTouchesCommand_setsPickKey(t *testing.T) {
	req, resp := buildGetAndTouchesCommand("gat", Rel(time.Second), []byte("only"))
	defer releaseReqAndResp(req, resp)
	equal(t, []byte("only"), req.key)

	req, resp = buildGetAndTouchesCommand("gat", Rel(time.Second), []byte("a"), []byte("b"))
	defer releaseReqAndResp(req, resp)
	isNil(t, req.key)
}

func Test_dataBlockValueUsesDeclaredLength(t *testing.T) {
	raw := []byte("ab\r\n")
	got, err := dataBlockValue(raw, 2)
	noError(t, err)
	equal(t, []byte("ab"), got)

	valueCRLF := []byte("\r\n\r\n")
	got, err = dataBlockValue(valueCRLF, 2)
	noError(t, err)
	equal(t, []byte("\r\n"), got)

	_, err = dataBlockValue([]byte("ab"), 2)
	isErrorIs(t, err, ErrMalformedResponse)
}
