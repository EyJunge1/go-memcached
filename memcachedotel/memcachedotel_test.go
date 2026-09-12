package memcachedotel_test

import (
	"context"
	"errors"
	"testing"
	"time"

	metricnoop "go.opentelemetry.io/otel/metric/noop"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/EyJunge1/go-memcached/memcached"
	"github.com/EyJunge1/go-memcached/memcachedotel"
)

func TestNewRequiresProvider(t *testing.T) {
	tests := []struct {
		name string
		opts []memcachedotel.Option
		want error
	}{
		{name: "no provider", want: memcachedotel.ErrNoProvider},
		{
			name: "nil tracer",
			opts: []memcachedotel.Option{memcachedotel.WithTracerProvider(nil)},
			want: memcachedotel.ErrNilTracerProvider,
		},
		{
			name: "nil meter",
			opts: []memcachedotel.Option{memcachedotel.WithMeterProvider(nil)},
			want: memcachedotel.ErrNilMeterProvider,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := memcachedotel.New(tt.opts...)
			if !errors.Is(err, tt.want) {
				t.Fatalf("err=%v want %v", err, tt.want)
			}
		})
	}
}

func TestObserveOpEmitsSpanAndMetrics(t *testing.T) {
	tp := &recordingTracerProvider{}
	mp := metricnoop.NewMeterProvider()

	inst, err := memcachedotel.New(
		memcachedotel.WithTracerProvider(tp),
		memcachedotel.WithMeterProvider(mp),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	inst.ObserveOp(ctx, memcached.OpEvent{
		Op:       "get",
		Server:   memcached.Server{Address: "127.0.0.1:11211"},
		Duration: 12 * time.Millisecond,
		Keys:     1,
	})

	spans := tp.ended()
	if len(spans) != 1 {
		t.Fatalf("spans=%d", len(spans))
	}
	if spans[0].Name != "memcached.get" {
		t.Fatalf("name=%q", spans[0].Name)
	}
	if d := spans[0].End.Sub(spans[0].Start); d < 10*time.Millisecond {
		t.Fatalf("span duration=%v", d)
	}
}

func TestPoolHooksRecordMetrics(t *testing.T) {
	mp := metricnoop.NewMeterProvider()

	inst, err := memcachedotel.New(memcachedotel.WithMeterProvider(mp))
	if err != nil {
		t.Fatal(err)
	}
	onAcquire, onCreate := inst.PoolHooks()
	if onAcquire == nil || onCreate == nil {
		t.Fatal("expected pool hooks")
	}
	ev := memcached.PoolEvent{
		Server:   memcached.Server{Address: "127.0.0.1:11211"},
		Duration: time.Millisecond,
	}
	onAcquire(ev)
	onCreate(ev)
}

func TestRegisterPoolStatsNilClient(t *testing.T) {
	inst, err := memcachedotel.New(memcachedotel.WithMeterProvider(metricnoop.NewMeterProvider()))
	if err != nil {
		t.Fatal(err)
	}
	if err := inst.RegisterPoolStats(nil); !errors.Is(err, memcachedotel.ErrNilClient) {
		t.Fatalf("err=%v want ErrNilClient", err)
	}
}

func TestInstrumentationImplementsObserver(t *testing.T) {
	inst, err := memcachedotel.New(memcachedotel.WithTracerProvider(tracenoop.NewTracerProvider()))
	if err != nil {
		t.Fatal(err)
	}
	var _ memcached.Observer = inst
}

func TestRetrySpansOptIn(t *testing.T) {
	tp := &recordingTracerProvider{}

	inst, err := memcachedotel.New(
		memcachedotel.WithTracerProvider(tp),
		memcachedotel.WithRetrySpans(),
	)
	if err != nil {
		t.Fatal(err)
	}
	inst.ObserveOp(context.Background(), memcached.OpEvent{
		Op:       "get",
		Duration: 30 * time.Millisecond,
		Keys:     1,
		Attempts: []memcached.Attempt{
			{Server: memcached.Server{Address: "127.0.0.1:11211"}, Duration: 10 * time.Millisecond, Err: context.DeadlineExceeded},
			{Server: memcached.Server{Address: "127.0.0.1:11211"}, Duration: 20 * time.Millisecond},
		},
	})
	spans := tp.ended()
	// children end before parent
	if len(spans) != 3 {
		t.Fatalf("spans=%d want 3 (2 attempts + parent)", len(spans))
	}
	parent := spans[len(spans)-1]
	if parent.Name != "memcached.get" {
		t.Fatalf("parent=%q", parent.Name)
	}
	retryAttr := false
	for _, a := range parent.Attrs {
		if string(a.Key) == "memcached.retry_count" && a.Value.AsInt64() == 1 {
			retryAttr = true
		}
	}
	if !retryAttr {
		t.Fatalf("missing retry_count=1 on %#v", parent.Attrs)
	}
}

func TestApplyAndHitsMissesBytes(t *testing.T) {
	tp := &recordingTracerProvider{}
	mp := metricnoop.NewMeterProvider()

	inst, err := memcachedotel.New(
		memcachedotel.WithTracerProvider(tp),
		memcachedotel.WithMeterProvider(mp),
		memcachedotel.WithKeyAttribute(),
	)
	if err != nil {
		t.Fatal(err)
	}

	var cfg memcached.Config
	inst.Apply(&cfg)
	if cfg.Observer != inst {
		t.Fatal("Apply did not set Observer")
	}
	if cfg.Pool.OnAcquire == nil || cfg.Pool.OnCreate == nil {
		t.Fatal("Apply did not set pool hooks")
	}

	inst.ObserveOp(context.Background(), memcached.OpEvent{
		Op:            "get",
		Duration:      time.Millisecond,
		Keys:          1,
		Hits:          1,
		ResponseBytes: 4,
		Key:           []byte("user:1"),
	})

	spans := tp.ended()
	if len(spans) != 1 {
		t.Fatalf("spans=%d", len(spans))
	}
	foundKey := false
	for _, a := range spans[0].Attrs {
		if string(a.Key) == "memcached.key" && a.Value.AsString() == "user:1" {
			foundKey = true
		}
	}
	if !foundKey {
		t.Fatalf("missing key attr: %#v", spans[0].Attrs)
	}
}
