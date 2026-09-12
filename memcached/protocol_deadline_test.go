package memcached

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestSelectProximateDeadline(t *testing.T) {
	mockNowFunc := func() time.Time {
		return time.Date(2021, 7, 5, 0, 0, 0, 0, time.UTC)
	}
	baseTime := mockNowFunc()

	tests := []struct {
		name         string
		ctx          context.Context
		conn         *mockConn
		timeout      time.Duration
		wantDeadline time.Time
		wantHas      bool
	}{
		{
			name:         "nil context and zero timeout",
			ctx:          nil,
			conn:         newMockConn(),
			timeout:      0,
			wantDeadline: time.Time{},
			wantHas:      false,
		},
		{
			name:         "nil context and negative timeout",
			ctx:          nil,
			conn:         newMockConn(),
			timeout:      -1 * time.Second,
			wantDeadline: time.Time{},
			wantHas:      false,
		},
		{
			name:         "background context and positive timeout",
			ctx:          context.Background(),
			conn:         newMockConn(),
			timeout:      time.Second,
			wantDeadline: baseTime.Add(time.Second),
			wantHas:      true,
		},
		{
			name: "context with deadline and zero timeout",
			ctx: func() context.Context {
				ctx, cancel := context.WithDeadline(context.Background(), baseTime.Add(2*time.Second))
				t.Cleanup(cancel)
				return ctx
			}(),
			conn:         newMockConn(),
			timeout:      0,
			wantDeadline: baseTime.Add(2 * time.Second),
			wantHas:      true,
		},
		{
			name: "context with earlier deadline",
			ctx: func() context.Context {
				ctx, cancel := context.WithDeadline(context.Background(), baseTime.Add(time.Second))
				t.Cleanup(cancel)
				return ctx
			}(),
			conn:         newMockConn(),
			timeout:      2 * time.Second,
			wantDeadline: baseTime.Add(time.Second),
			wantHas:      true,
		},
		{
			name: "timeout earlier than context deadline",
			ctx: func() context.Context {
				ctx, cancel := context.WithDeadline(context.Background(), baseTime.Add(2*time.Second))
				t.Cleanup(cancel)
				return ctx
			}(),
			conn:         newMockConn(),
			timeout:      time.Second,
			wantDeadline: baseTime.Add(time.Second),
			wantHas:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotHas := selectProximateDeadline(tt.ctx, tt.conn, tt.timeout, mockNowFunc, true)
			equal(t, tt.wantHas, gotHas)
			equal(t, tt.wantDeadline, tt.conn.readDeadline)
		})
	}
}

func TestPrepareOpIOBackgroundIsNop(t *testing.T) {
	cn := newMockConn()
	stop := prepareOpIO(context.Background(), cn, time.Second, time.Second)
	stop()
	equal(t, 1, cn.deadlineCalls)
}

func TestApplyOpDeadlinesCombined(t *testing.T) {
	mockNow := func() time.Time {
		return time.Date(2021, 7, 5, 0, 0, 0, 0, time.UTC)
	}
	cn := newMockConn()
	applyOpDeadlinesAt(context.Background(), cn, time.Second, time.Second, mockNow)
	want := mockNow().Add(time.Second)
	equal(t, want, cn.readDeadline)
	equal(t, want, cn.writeDeadline)
	equal(t, 1, cn.deadlineCalls)
	equal(t, 0, cn.readDeadlineCalls)
	equal(t, 0, cn.writeDeadlineCalls)
}

func TestApplyOpDeadlinesSplit(t *testing.T) {
	mockNow := func() time.Time {
		return time.Date(2021, 7, 5, 0, 0, 0, 0, time.UTC)
	}
	cn := newMockConn()
	applyOpDeadlinesAt(context.Background(), cn, 2*time.Second, time.Second, mockNow)
	equal(t, mockNow().Add(time.Second), cn.writeDeadline)
	equal(t, mockNow().Add(2*time.Second), cn.readDeadline)
	equal(t, 0, cn.deadlineCalls)
	equal(t, 1, cn.readDeadlineCalls)
	equal(t, 1, cn.writeDeadlineCalls)
}

func TestApplyOpDeadlinesClearsCombinedDeadline(t *testing.T) {
	cn := newMockConn()
	cn.readDeadline = time.Now()
	cn.writeDeadline = time.Now()

	applyOpDeadlinesAt(context.Background(), cn, 0, 0, time.Now)

	isTrue(t, cn.readDeadline.IsZero())
	isTrue(t, cn.writeDeadline.IsZero())
	equal(t, 1, cn.deadlineCalls)
}

func TestApplyOpDeadlinesClearsDisabledHalf(t *testing.T) {
	now := time.Now()
	cn := newMockConn()
	cn.readDeadline = now
	cn.writeDeadline = now

	applyOpDeadlinesAt(context.Background(), cn, 0, time.Second, func() time.Time { return now })

	isTrue(t, cn.readDeadline.IsZero())
	equal(t, now.Add(time.Second), cn.writeDeadline)
	equal(t, 1, cn.readDeadlineCalls)
	equal(t, 1, cn.writeDeadlineCalls)
}

func TestCappedValueSize(t *testing.T) {
	n, err := cappedValueSize(4, 0)
	noError(t, err)
	equal(t, 4, n)

	_, err = cappedValueSize(8, 4)
	isErrorIs(t, err, ErrMalformedResponse)

	_, err = cappedValueSize(uint64(math.MaxInt)+1, math.MaxInt)
	isErrorIs(t, err, ErrMalformedResponse)
}
