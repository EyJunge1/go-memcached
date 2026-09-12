package memcachedotel_test

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/embedded"
)

// recordingTracerProvider records ended spans without the OTel SDK.
type recordingTracerProvider struct {
	embedded.TracerProvider

	mu    sync.Mutex
	spans []recordedSpan
}

type recordedSpan struct {
	Name  string
	Start time.Time
	End   time.Time
	Attrs []attribute.KeyValue
}

func (p *recordingTracerProvider) Tracer(string, ...trace.TracerOption) trace.Tracer {
	return &recordingTracer{p: p}
}

func (p *recordingTracerProvider) ended() []recordedSpan {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]recordedSpan, len(p.spans))
	copy(out, p.spans)
	return out
}

type recordingTracer struct {
	embedded.Tracer

	p *recordingTracerProvider
}

func (t *recordingTracer) Start(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	cfg := trace.NewSpanStartConfig(opts...)
	start := time.Now()
	if ts := cfg.Timestamp(); !ts.IsZero() {
		start = ts
	}
	attrs := append([]attribute.KeyValue(nil), cfg.Attributes()...)
	s := &recordingSpan{p: t.p, name: name, start: start, attrs: attrs}
	return trace.ContextWithSpan(ctx, s), s
}

type recordingSpan struct {
	embedded.Span

	p     *recordingTracerProvider
	name  string
	start time.Time
	attrs []attribute.KeyValue
}

func (s *recordingSpan) End(opts ...trace.SpanEndOption) {
	cfg := trace.NewSpanEndConfig(opts...)
	end := time.Now()
	if ts := cfg.Timestamp(); !ts.IsZero() {
		end = ts
	}
	s.p.mu.Lock()
	defer s.p.mu.Unlock()
	s.p.spans = append(s.p.spans, recordedSpan{
		Name:  s.name,
		Start: s.start,
		End:   end,
		Attrs: append([]attribute.KeyValue(nil), s.attrs...),
	})
}

func (s *recordingSpan) AddEvent(string, ...trace.EventOption)   {}
func (s *recordingSpan) AddLink(trace.Link)                      {}
func (s *recordingSpan) IsRecording() bool                       { return true }
func (s *recordingSpan) RecordError(error, ...trace.EventOption) {}
func (s *recordingSpan) SpanContext() trace.SpanContext          { return trace.SpanContext{} }
func (s *recordingSpan) SetStatus(codes.Code, string)            {}
func (s *recordingSpan) SetName(string)                          {}
func (s *recordingSpan) TracerProvider() trace.TracerProvider    { return s.p }
func (s *recordingSpan) SetAttributes(kv ...attribute.KeyValue) {
	s.attrs = append(s.attrs, kv...)
}
