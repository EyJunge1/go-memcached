package memcachedotel_test

import (
	"fmt"

	metricnoop "go.opentelemetry.io/otel/metric/noop"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/EyJunge1/go-memcached/memcached"
	"github.com/EyJunge1/go-memcached/memcachedotel"
)

func Example() {
	// Pass your app's TracerProvider / MeterProvider (SDK + exporters).
	// noop is only for this example so the module stays SDK-free.
	tp := tracenoop.NewTracerProvider()
	mp := metricnoop.NewMeterProvider()

	obs, err := memcachedotel.New(
		memcachedotel.WithTracerProvider(tp),
		memcachedotel.WithMeterProvider(mp),
	)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = obs.Close() }()

	cfg := memcached.Config{
		Servers: []memcached.Server{{Address: "localhost:11211"}},
	}
	obs.Apply(&cfg)

	client, err := memcached.New(cfg)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = client.Close() }()

	_ = obs.RegisterPoolStats(client)
	_ = client
}
