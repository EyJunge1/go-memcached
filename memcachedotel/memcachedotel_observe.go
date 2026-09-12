package memcachedotel

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/EyJunge1/go-memcached/memcached"
)

// Apply sets cfg.Observer and wires Pool.OnAcquire/OnCreate (chaining any
// existing hooks). Prefer this over manual PoolHooks assignment.
func (i *Instrumentation) Apply(cfg *memcached.Config) {
	if i == nil || cfg == nil {
		return
	}
	cfg.Observer = i
	if onAcquire, onCreate := i.PoolHooks(); onAcquire != nil {
		prevA, prevC := cfg.Pool.OnAcquire, cfg.Pool.OnCreate
		cfg.Pool.OnAcquire = func(e memcached.PoolEvent) {
			onAcquire(e)
			if prevA != nil {
				prevA(e)
			}
		}
		cfg.Pool.OnCreate = func(e memcached.PoolEvent) {
			onCreate(e)
			if prevC != nil {
				prevC(e)
			}
		}
	}
}

// RegisterPoolStats registers observable gauges that sample client.PoolStats.
// Call after memcached.New. Safe to call once; subsequent calls replace the registration.
func (i *Instrumentation) RegisterPoolStats(client *memcached.Client) error {
	if i == nil || i.metrics == nil || i.meter == nil {
		return nil
	}
	if client == nil {
		return ErrNilClient
	}
	i.poolMu.Lock()
	defer i.poolMu.Unlock()
	if i.poolReg != nil {
		if err := i.poolReg.Unregister(); err != nil {
			return fmt.Errorf("memcachedotel: unregister pool stats: %w", err)
		}
		i.poolReg = nil
	}
	i.poolClient = client
	reg, err := i.meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		c := func() *memcached.Client {
			i.poolMu.Lock()
			defer i.poolMu.Unlock()
			return i.poolClient
		}()
		if c == nil {
			return nil
		}
		for _, s := range c.PoolStats() {
			base := serverAttributes(s.Server)
			totalAttrs := make([]attribute.KeyValue, 0, len(base)+1)
			totalAttrs = append(totalAttrs, base...)
			totalAttrs = append(totalAttrs, attribute.String("pool.state", "total"))
			idleAttrs := make([]attribute.KeyValue, 0, len(base)+1)
			idleAttrs = append(idleAttrs, base...)
			idleAttrs = append(idleAttrs, attribute.String("pool.state", "idle"))
			o.ObserveInt64(i.metrics.poolConns, int64(s.Total), metric.WithAttributes(totalAttrs...))
			o.ObserveInt64(i.metrics.poolConns, int64(s.Idle), metric.WithAttributes(idleAttrs...))
		}
		return nil
	}, i.metrics.poolConns)
	if err != nil {
		return fmt.Errorf("memcachedotel: register pool stats: %w", err)
	}
	i.poolReg = reg
	return nil
}

// Close unregisters pool-stat callbacks.
func (i *Instrumentation) Close() error {
	if i == nil {
		return nil
	}
	i.poolMu.Lock()
	defer i.poolMu.Unlock()
	if i.poolReg != nil {
		if err := i.poolReg.Unregister(); err != nil {
			i.poolReg = nil
			i.poolClient = nil
			return fmt.Errorf("memcachedotel: unregister pool stats: %w", err)
		}
		i.poolReg = nil
		i.poolClient = nil
	}
	return nil
}

// ObserveOp implements memcached.Observer.
func (i *Instrumentation) ObserveOp(ctx context.Context, e memcached.OpEvent) {
	if i == nil {
		return
	}
	attrs := opAttributes(e, i.includeKeys)
	if i.tracer != nil {
		end := time.Now()
		start := end.Add(-e.Duration)
		spanCtx, span := i.tracer.Start(ctx, "memcached."+e.Op,
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithTimestamp(start),
			trace.WithAttributes(attrs...),
		)
		span.SetStatus(codes.Ok, "")
		if e.Err != nil {
			span.RecordError(e.Err)
			span.SetStatus(codes.Error, e.Err.Error())
		}
		if i.retrySpans && e.Retries() > 0 {
			i.emitRetrySpans(spanCtx, e, start)
		}
		span.End(trace.WithTimestamp(end))
	}
	if i.metrics != nil {
		o := metric.WithAttributes(attrs...)
		i.metrics.opCalls.Add(ctx, 1, o)
		i.metrics.opDuration.Record(ctx, e.Duration.Seconds(), o)
		if e.Err != nil {
			i.metrics.opErrors.Add(ctx, 1, o)
		}
		if e.Hits > 0 {
			i.metrics.opHits.Add(ctx, int64(e.Hits), o)
		}
		if e.Misses > 0 {
			i.metrics.opMisses.Add(ctx, int64(e.Misses), o)
		}
		if e.RequestBytes > 0 {
			i.metrics.opReqBytes.Record(ctx, e.RequestBytes, o)
		}
		if e.ResponseBytes > 0 {
			i.metrics.opRespBytes.Record(ctx, e.ResponseBytes, o)
		}
		if r := e.Retries(); r > 0 {
			i.metrics.opRetries.Add(ctx, int64(r), o)
		}
	}
}

func (i *Instrumentation) emitRetrySpans(ctx context.Context, e memcached.OpEvent, parentStart time.Time) {
	t := parentStart
	for idx, a := range e.Attempts {
		aEnd := t.Add(a.Duration)
		serverAttrs := serverAttributes(a.Server)
		attrs := make([]attribute.KeyValue, 0, 3+len(serverAttrs))
		attrs = append(attrs,
			semconv.DBSystemNameMemcached,
			semconv.DBOperationName(e.Op),
			attribute.Int("memcached.attempt", idx),
		)
		attrs = append(attrs, serverAttrs...)
		_, span := i.tracer.Start(ctx, "memcached."+e.Op+".attempt",
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithTimestamp(t),
			trace.WithAttributes(attrs...),
		)
		span.SetStatus(codes.Ok, "")
		if a.Err != nil {
			span.RecordError(a.Err)
			span.SetStatus(codes.Error, a.Err.Error())
		}
		span.End(trace.WithTimestamp(aEnd))
		t = aEnd
	}
}

// PoolHooks returns OnAcquire/OnCreate callbacks for memcached.Pool.
// Prefer Apply, which wires them automatically.
func (i *Instrumentation) PoolHooks() (onAcquire, onCreate func(memcached.PoolEvent)) {
	if i == nil || i.metrics == nil {
		return nil, nil
	}
	return func(e memcached.PoolEvent) {
			i.recordPool(context.Background(), e, true /* acquire */)
		}, func(e memcached.PoolEvent) {
			i.recordPool(context.Background(), e, false /* create */)
		}
}

func (i *Instrumentation) recordPool(ctx context.Context, e memcached.PoolEvent, acquire bool) {
	attrs := serverAttributes(e.Server)
	o := metric.WithAttributes(attrs...)
	sec := e.Duration.Seconds()
	if acquire {
		i.metrics.poolAcquireCalls.Add(ctx, 1, o)
		i.metrics.poolAcquire.Record(ctx, sec, o)
		return
	}
	i.metrics.poolCreateCalls.Add(ctx, 1, o)
	i.metrics.poolCreate.Record(ctx, sec, o)
}

func opAttributes(e memcached.OpEvent, includeKeys bool) []attribute.KeyValue {
	serverAttrs := serverAttributes(e.Server)
	n := 4 + len(serverAttrs)
	if e.Hits > 0 || e.Misses > 0 {
		n += 2
	}
	if e.RequestBytes > 0 {
		n++
	}
	if e.ResponseBytes > 0 {
		n++
	}
	if includeKeys && len(e.Key) > 0 {
		n++
	}
	attrs := make([]attribute.KeyValue, 0, n)
	attrs = append(attrs,
		semconv.DBSystemNameMemcached,
		semconv.DBOperationName(e.Op),
		attribute.Int("db.operation.batch.size", e.Keys),
		attribute.Int("memcached.retry_count", e.Retries()),
	)
	if e.Hits > 0 || e.Misses > 0 {
		attrs = append(attrs,
			attribute.Int("memcached.hits", e.Hits),
			attribute.Int("memcached.misses", e.Misses),
		)
	}
	if e.RequestBytes > 0 {
		attrs = append(attrs, attribute.Int64("memcached.request.size", e.RequestBytes))
	}
	if e.ResponseBytes > 0 {
		attrs = append(attrs, attribute.Int64("memcached.response.size", e.ResponseBytes))
	}
	if includeKeys && len(e.Key) > 0 {
		attrs = append(attrs, attribute.String("memcached.key", string(e.Key)))
	}
	attrs = append(attrs, serverAttrs...)
	return attrs
}

func serverAttributes(s memcached.Server) []attribute.KeyValue {
	if s.Address == "" {
		return nil
	}
	host, portStr, err := net.SplitHostPort(s.Address)
	if err != nil {
		return []attribute.KeyValue{semconv.ServerAddress(s.Address)}
	}
	attrs := make([]attribute.KeyValue, 0, 3)
	attrs = append(attrs, semconv.ServerAddress(host))
	if port, err := strconv.Atoi(portStr); err == nil {
		attrs = append(attrs, semconv.ServerPort(port))
	}
	if s.Network != "" && s.Network != "tcp" {
		attrs = append(attrs, attribute.String("network.transport", s.Network))
	}
	return attrs
}
