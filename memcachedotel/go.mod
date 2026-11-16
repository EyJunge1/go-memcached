module github.com/EyJunge1/go-memcached/memcachedotel

go 1.27

replace github.com/EyJunge1/go-memcached => ../

// After tagging the root module (e.g. v0.1.0), bump the core require to that
// SemVer. replace => ../ keeps local development working until then.
require (
	github.com/EyJunge1/go-memcached v0.0.0-00010101000000-000000000000
	go.opentelemetry.io/otel v1.47.0
	go.opentelemetry.io/otel/metric v1.47.0
	go.opentelemetry.io/otel/trace v1.47.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/klauspost/cpuid/v2 v2.4.2 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	golang.org/x/sys v0.50.0 // indirect
)
