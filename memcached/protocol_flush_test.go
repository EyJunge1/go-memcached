package memcached

import (
	"testing"
	"time"
)

func TestBuildFlushAllCommandDelay(t *testing.T) {
	tests := []struct {
		name    string
		delay   Exptime
		noreply bool
		wantRaw []byte
		wantEnd responseEndIndicator
	}{
		{
			name:    "no delay",
			wantRaw: []byte("flush_all\r\n"),
			wantEnd: _endIndicatorLimitedLines,
		},
		{
			name:    "delay 10",
			delay:   10,
			wantRaw: []byte("flush_all 10\r\n"),
			wantEnd: _endIndicatorLimitedLines,
		},
		{
			name:    "rel 10s",
			delay:   Rel(10 * time.Second),
			wantRaw: []byte("flush_all 10\r\n"),
			wantEnd: _endIndicatorLimitedLines,
		},
		{
			name:    "immediate",
			delay:   ExptimeImmediate,
			wantRaw: []byte("flush_all -1\r\n"),
			wantEnd: _endIndicatorLimitedLines,
		},
		{
			name:    "noreply",
			delay:   10,
			noreply: true,
			wantRaw: []byte("flush_all 10 noreply\r\n"),
			wantEnd: _endIndicatorNoReply,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, resp, _ := buildFlushAllCommand(tt.delay, nil, tt.noreply)
			defer releaseReqAndResp(req, resp)
			equal(t, tt.wantRaw, req.raw)
			equal(t, tt.wantEnd, resp.endIndicator)
		})
	}
}
