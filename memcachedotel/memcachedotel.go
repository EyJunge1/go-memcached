// Package memcachedotel implements memcached.Observer with OpenTelemetry traces and metrics.
//
// Import this module only when you want OTel; the core client stays free of
// go.opentelemetry.io dependencies. Pass TracerProvider and MeterProvider from
// the application SDK; exporters (OTLP HTTP/gRPC, …) are not configured here.
package memcachedotel

import (
	"errors"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/EyJunge1/go-memcached/memcached"
)

// Sentinel errors returned by New and RegisterPoolStats.
var (
	ErrNoProvider        = errors.New("memcachedotel: set WithTracerProvider and/or WithMeterProvider")
	ErrNilTracerProvider = errors.New("memcachedotel: WithTracerProvider requires a non-nil TracerProvider")
	ErrNilMeterProvider  = errors.New("memcachedotel: WithMeterProvider requires a non-nil MeterProvider")
	ErrNilClient         = errors.New("memcachedotel: nil client")
)

const (
	_instrumentationName    = "github.com/EyJunge1/go-memcached/memcachedotel"
	_instrumentationVersion = "1.0.0"

	_metricOpDuration   = "memcached.client.operation.duration"
	_metricOpCalls      = "memcached.client.operation.calls"
	_metricOpErrors     = "memcached.client.operation.errors"
	_metricOpHits       = "memcached.client.operation.hits"
	_metricOpMisses     = "memcached.client.operation.misses"
	_metricOpReqBytes   = "memcached.client.operation.request.size"
	_metricOpRespBytes  = "memcached.client.operation.response.size"
	_metricPoolAcquire  = "memcached.client.pool.acquire.duration"
	_metricPoolCreate   = "memcached.client.pool.create.duration"
	_metricPoolAcquireN = "memcached.client.pool.acquire.calls"
	_metricPoolCreateN  = "memcached.client.pool.create.calls"
	_metricPoolConns    = "memcached.client.pool.connections"
	_metricOpRetries    = "memcached.client.operation.retries"
)

type (
	// Option configures New.
	Option interface {
		apply(*options)
	}

	options struct {
		tracerProvider trace.TracerProvider
		meterProvider  metric.MeterProvider
		enableTrace    bool
		enableMetric   bool
		includeKeys    bool
		retrySpans     bool
	}
)

type tracerProviderOption struct {
	tp trace.TracerProvider
}

func (o tracerProviderOption) apply(opts *options) {
	opts.enableTrace = true
	opts.tracerProvider = o.tp
}

type meterProviderOption struct {
	mp metric.MeterProvider
}

func (o meterProviderOption) apply(opts *options) {
	opts.enableMetric = true
	opts.meterProvider = o.mp
}

type keyAttributeOption struct{}

func (keyAttributeOption) apply(opts *options) {
	opts.includeKeys = true
}

type retrySpansOption struct{}

func (retrySpansOption) apply(opts *options) {
	opts.retrySpans = true
}

var (
	_ Option = tracerProviderOption{}
	_ Option = meterProviderOption{}
	_ Option = keyAttributeOption{}
	_ Option = retrySpansOption{}
)

// Instrumentation is a memcached.Observer backed by OpenTelemetry.
type Instrumentation struct {
	tracer      trace.Tracer
	metrics     *clientMetrics
	meter       metric.Meter
	includeKeys bool
	retrySpans  bool

	poolMu     sync.Mutex
	poolClient *memcached.Client
	poolReg    metric.Registration
}

var _ memcached.Observer = (*Instrumentation)(nil)

type clientMetrics struct {
	opDuration       metric.Float64Histogram
	opCalls          metric.Int64Counter
	opErrors         metric.Int64Counter
	opHits           metric.Int64Counter
	opMisses         metric.Int64Counter
	opReqBytes       metric.Int64Histogram
	opRespBytes      metric.Int64Histogram
	opRetries        metric.Int64Counter
	poolAcquire      metric.Float64Histogram
	poolCreate       metric.Float64Histogram
	poolAcquireCalls metric.Int64Counter
	poolCreateCalls  metric.Int64Counter
	poolConns        metric.Int64ObservableGauge
}

// New builds an Observer. Call WithTracerProvider and/or WithMeterProvider.
func New(opts ...Option) (*Instrumentation, error) {
	var o options
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt.apply(&o)
	}
	if !o.enableTrace && !o.enableMetric {
		return nil, ErrNoProvider
	}

	inst := &Instrumentation{includeKeys: o.includeKeys, retrySpans: o.retrySpans}
	if o.enableTrace {
		if o.tracerProvider == nil {
			return nil, ErrNilTracerProvider
		}
		inst.tracer = o.tracerProvider.Tracer(
			_instrumentationName,
			trace.WithInstrumentationVersion(_instrumentationVersion),
		)
	}
	if o.enableMetric {
		if o.meterProvider == nil {
			return nil, ErrNilMeterProvider
		}
		inst.meter = o.meterProvider.Meter(
			_instrumentationName,
			metric.WithInstrumentationVersion(_instrumentationVersion),
		)
		m, err := newClientMetrics(inst.meter)
		if err != nil {
			return nil, fmt.Errorf("memcachedotel: metrics: %w", err)
		}
		inst.metrics = m
	}
	return inst, nil
}

// WithTracerProvider enables tracing. tp must be non-nil.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return tracerProviderOption{tp: tp}
}

// WithMeterProvider enables metrics. mp must be non-nil.
func WithMeterProvider(mp metric.MeterProvider) Option {
	return meterProviderOption{mp: mp}
}

// WithKeyAttribute records OpEvent.Key on spans/metrics when present.
// Off by default (keys may be sensitive).
func WithKeyAttribute() Option {
	return keyAttributeOption{}
}

// WithRetrySpans emits a child span per transport attempt when Retries()>0.
// Off by default; retry_count is still recorded on the parent span/metrics.
func WithRetrySpans() Option {
	return retrySpansOption{}
}

func newClientMetrics(meter metric.Meter) (*clientMetrics, error) {
	opDuration, err := meter.Float64Histogram(
		_metricOpDuration,
		metric.WithUnit("s"),
		metric.WithDescription("Duration of memcached client operations"),
	)
	if err != nil {
		return nil, err
	}
	opCalls, err := meter.Int64Counter(
		_metricOpCalls,
		metric.WithUnit("{call}"),
		metric.WithDescription("Number of memcached client operations"),
	)
	if err != nil {
		return nil, err
	}
	opErrors, err := meter.Int64Counter(
		_metricOpErrors,
		metric.WithUnit("{error}"),
		metric.WithDescription("Number of memcached client operation errors"),
	)
	if err != nil {
		return nil, err
	}
	opHits, err := meter.Int64Counter(
		_metricOpHits,
		metric.WithUnit("{hit}"),
		metric.WithDescription("Cache hits on retrieval operations"),
	)
	if err != nil {
		return nil, err
	}
	opMisses, err := meter.Int64Counter(
		_metricOpMisses,
		metric.WithUnit("{miss}"),
		metric.WithDescription("Cache misses on retrieval operations"),
	)
	if err != nil {
		return nil, err
	}
	opReqBytes, err := meter.Int64Histogram(
		_metricOpReqBytes,
		metric.WithUnit("By"),
		metric.WithDescription("Outbound request payload size"),
	)
	if err != nil {
		return nil, err
	}
	opRespBytes, err := meter.Int64Histogram(
		_metricOpRespBytes,
		metric.WithUnit("By"),
		metric.WithDescription("Inbound response value size"),
	)
	if err != nil {
		return nil, err
	}
	opRetries, err := meter.Int64Counter(
		_metricOpRetries,
		metric.WithUnit("{retry}"),
		metric.WithDescription("Number of transport retries after the first attempt"),
	)
	if err != nil {
		return nil, err
	}
	poolAcquire, err := meter.Float64Histogram(
		_metricPoolAcquire,
		metric.WithUnit("s"),
		metric.WithDescription("Duration of connection pool acquire"),
	)
	if err != nil {
		return nil, err
	}
	poolCreate, err := meter.Float64Histogram(
		_metricPoolCreate,
		metric.WithUnit("s"),
		metric.WithDescription("Duration of connection dial/create"),
	)
	if err != nil {
		return nil, err
	}
	poolAcquireCalls, err := meter.Int64Counter(
		_metricPoolAcquireN,
		metric.WithUnit("{call}"),
		metric.WithDescription("Number of connection pool acquires"),
	)
	if err != nil {
		return nil, err
	}
	poolCreateCalls, err := meter.Int64Counter(
		_metricPoolCreateN,
		metric.WithUnit("{call}"),
		metric.WithDescription("Number of connection dial/create attempts"),
	)
	if err != nil {
		return nil, err
	}
	poolConns, err := meter.Int64ObservableGauge(
		_metricPoolConns,
		metric.WithUnit("{connection}"),
		metric.WithDescription("Connection pool occupancy (total and idle)"),
	)
	if err != nil {
		return nil, err
	}
	return &clientMetrics{
		opDuration:       opDuration,
		opCalls:          opCalls,
		opErrors:         opErrors,
		opHits:           opHits,
		opMisses:         opMisses,
		opReqBytes:       opReqBytes,
		opRespBytes:      opRespBytes,
		opRetries:        opRetries,
		poolAcquire:      poolAcquire,
		poolCreate:       poolCreate,
		poolAcquireCalls: poolAcquireCalls,
		poolCreateCalls:  poolCreateCalls,
		poolConns:        poolConns,
	}, nil
}
