package memcached

import (
	"bytes"
	"math"
	"testing"
	"time"
)

func TestValidateKeyAndValue(t *testing.T) {
	tests := []struct {
		name    string
		key     []byte
		value   []byte
		wantErr error
	}{
		{name: "ok", key: []byte("key"), value: []byte("value")},
		{name: "nil value", key: []byte("key")},
		{name: "nil key", value: []byte("value"), wantErr: ErrInvalidKey},
		{name: "empty key", key: []byte(""), value: []byte("value"), wantErr: ErrInvalidKey},
		{name: "too long", key: bytes.Repeat([]byte("a"), _maxKeySize+1), wantErr: ErrInvalidKey},
		{name: "space", key: []byte("has space"), wantErr: ErrInvalidKey},
		{name: "tab", key: []byte("has\ttab"), wantErr: ErrInvalidKey},
		{name: "del", key: []byte{0x7f}, wantErr: ErrInvalidKey},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateKeyAndValue(tt.key, tt.value)
			isErrorIs(t, err, tt.wantErr)
		})
	}
	equal(t, 250, _maxKeySize)
}

func TestValidateKeyAndValueFlagsBinary(t *testing.T) {
	tests := []struct {
		name    string
		key     []byte
		wantErr error
	}{
		{name: "space ok", key: []byte("has space")},
		{name: "too long", key: bytes.Repeat([]byte("a"), 188), wantErr: ErrInvalidKey},
		{name: "max", key: bytes.Repeat([]byte("a"), 186)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateKeyAndValueFlags(tt.key, nil, true /* binary */)
			isErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestRel(t *testing.T) {
	tests := []struct {
		name string
		in   time.Duration
		want Exptime
	}{
		{name: "never", in: 0, want: ExptimeNever},
		{name: "immediate", in: -time.Second, want: ExptimeImmediate},
		{name: "subsecond", in: 500 * time.Millisecond, want: Exptime(1)},
		{name: "one second", in: time.Second, want: Exptime(1)},
		{name: "minute", in: 60 * time.Second, want: Exptime(60)},
		{name: "thirty days", in: 30 * 24 * time.Hour, want: Exptime(2592000)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			equal(t, tt.want, Rel(tt.in))
		})
	}

	fixed := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	want := Exptime(fixed.Add(31 * 24 * time.Hour).Unix())
	equal(t, want, relAt(31*24*time.Hour, fixed))
}

func TestUnixExp(t *testing.T) {
	tests := []struct {
		name    string
		unix    int64
		want    Exptime
		wantErr error
	}{
		{name: "too small", unix: 2592000, wantErr: ErrInvalidArgument},
		{name: "ok", unix: 2592001, want: Exptime(2592001)},
		{name: "overflow", unix: int64(math.MaxInt32) + 1, wantErr: ErrInvalidArgument},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := UnixExp(time.Unix(tt.unix, 0))
			if tt.wantErr != nil {
				isErrorIs(t, err, tt.wantErr)
				return
			}
			noError(t, err)
			equal(t, tt.want, got)
		})
	}
}

func TestExpiryToken(t *testing.T) {
	tests := []struct {
		name string
		in   Exptime
		want int64
	}{
		{name: "never", in: ExptimeNever},
		{name: "immediate", in: ExptimeImmediate, want: -1},
		{name: "sixty", in: 60, want: 60},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			equal(t, tt.want, expiryToken(tt.in))
		})
	}
}

func TestRecacheToken(t *testing.T) {
	tests := []struct {
		name   string
		in     time.Duration
		want   int64
		wantOK bool
	}{
		{name: "zero"},
		{name: "negative", in: -time.Second},
		{name: "thirty one days", in: 31 * 24 * time.Hour, want: 2678400, wantOK: true},
		{name: "five seconds", in: 5 * time.Second, want: 5, wantOK: true},
		{name: "subsecond", in: 500 * time.Millisecond, want: 1, wantOK: true},
		{name: "thirty days", in: 30 * 24 * time.Hour, want: 2592000, wantOK: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tok, ok := recacheToken(tt.in)
			equal(t, tt.wantOK, ok)
			if tt.wantOK {
				equal(t, tt.want, tok)
			}
		})
	}
}

func TestBuildStorageCommandExpiryWire(t *testing.T) {
	tests := []struct {
		name string
		exp  Exptime
		want []byte
	}{
		{
			name: "rounded second",
			exp:  Rel(500 * time.Millisecond),
			want: []byte("set k 0 1 1\r\nv\r\n"),
		},
		{
			name: "immediate",
			exp:  ExptimeImmediate,
			want: []byte("set k 0 -1 1\r\nv\r\n"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, resp, _ := buildStorageCommand(_SetBytes, []byte("k"), []byte("v"), 0, tt.exp, nil, false)
			defer releaseReqAndResp(req, resp)
			equal(t, tt.want, req.wire())
		})
	}
}

func TestMapStorageReply(t *testing.T) {
	tests := []struct {
		name          string
		resp          *response
		wantErr       error
		shouldDiscard bool
	}{
		{
			name: "stored",
			resp: &response{endIndicator: _endIndicatorLimitedLines, rawLines: [][]byte{_StoredCRLFBytes}},
		},
		{
			name:    "not stored",
			resp:    &response{endIndicator: _endIndicatorLimitedLines, rawLines: [][]byte{[]byte("NOT_STORED\r\n")}},
			wantErr: ErrNotStored,
		},
		{
			name:    "exists",
			resp:    &response{endIndicator: _endIndicatorLimitedLines, rawLines: [][]byte{[]byte("EXISTS\r\n")}},
			wantErr: ErrExists,
		},
		{
			name:    "not found",
			resp:    &response{endIndicator: _endIndicatorLimitedLines, rawLines: [][]byte{[]byte("NOT_FOUND\r\n")}},
			wantErr: ErrNotFound,
		},
		{
			name:          "junk",
			resp:          &response{endIndicator: _endIndicatorLimitedLines, rawLines: [][]byte{[]byte("WHATEVER\r\n")}},
			wantErr:       ErrMalformedResponse,
			shouldDiscard: true,
		},
		{
			name: "quiet",
			resp: &response{endIndicator: _endIndicatorNoReply},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := mapStorageReply(tt.resp)
			if tt.wantErr != nil {
				isErrorIs(t, err, tt.wantErr)
				equal(t, tt.shouldDiscard, shouldDiscardConn(wrapRecvError(err)))
				return
			}
			noError(t, err)
		})
	}
}
