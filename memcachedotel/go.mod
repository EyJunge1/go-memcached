module github.com/EyJunge1/go-memcached/memcachedotel

go 1.27

replace github.com/EyJunge1/go-memcached => ../

// replace => ../ keeps local development/CI on the tree; published module
// consumers resolve the SemVer require below.
require (
	github.com/EyJunge1/go-memcached v0.1.0
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
