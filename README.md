# gomemcached

Go client for [Memcached](https://memcached.org/), based on [yeqown/memcached](https://github.com/yeqown/memcached).

### Features

- [x] Completed Memcached text protocol, includes meta text protocol.
- Values are raw `[]byte`; encode or compress in the caller before `Set`
- [x] Cluster support, multiple hash algorithm support, include: crc32, murmur3, rendezvous and also custom hash algorithm.
- [x] Fully connection pool features support.
- [x] <del>SASL support.</del>
- [x] support TCP、UDP and Unix domain socket transport.

### Installation

```bash
go get github.com/EyJunge1/gomemcached@latest
```

### Usage

```go
package main

import (
    "context"
    "time"

    "github.com/EyJunge1/gomemcached"
)

func main() {
	// 1. build client
	// addrs is a string, if you have multiple memcached servers, you can use comma to separate them.
	// e.g. "localhost:11211,localhost:11212,localhost:11213"
	addrs := "localhost:11211"

	// client support options, you can set the options to client.
	// e.g. memcached.New(addrs, memcached.WithDialTimeout(3*time.Second))
	client, err := memcached.New(addrs)
	if err != nil {
		panic(err)
	}

	// 2. use client
	// now, you can use the client API to finish your work.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	version, err := client.Version(ctx)
	if err != nil {
		panic(err)
	}
	println("Version: ", version)

	if err = client.Set(ctx, "key", []byte("value"), 0, 0); err != nil {
		panic(err)
	}
	if err = client.Set(ctx, "key2", []byte("value2"), 0, 0); err != nil {
		panic(err)
	}

	items, err := client.Gets(ctx, "key", "key2")
	if err != nil {
		panic(err)
	}

	for _, item := range items {
		println("key: ", item.Key, " value: ", string(item.Value))
	}
}
```

### Values and flags

The client stores and returns `[]byte` and a `uint32` flags field as-is. Encode (JSON, Protobuf, …) or compress in your application before `Set`; decode after `Get`. The library does not pull serialization or compression dependencies.

### Support Commands

The public API exposes typed text- and meta-protocol commands only. There is no
arbitrary raw command passthrough; callers should use the methods below.

Now, we have implemented some commands, and we will implement more commands in the future.

| Command        | Status | API Usage                                                                                                           | Description                                                       |
|----------------|--------|---------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------|
| ----           | -----  | STORAGE COMMANDS                                                                                                    | ---                                                               |
| Set            | ✅      | `Set(ctx context.Context, key string, value []byte, flag uint32, expiry time.Duration) error`                   | Set a key-value pair to memcached                                 |
| Add            | ✅      | `Add(ctx context.Context, key string, value []byte, flag uint32, expiry time.Duration) error`                   | Add a key-value pair to memcached                                 |
| Replace        | ✅      | `Replace(ctx context.Context, key string, value []byte, flag uint32, expiry time.Duration) error`               | Replace a key-value pair to memcached                             |
| Append         | ✅      | `Append(ctx context.Context, key string, value []byte, flag uint32, expiry time.Duration) error`                | Append a value to the key                                         |
| Prepend        | ✅      | `Prepend(ctx context.Context, key string, value []byte, flag uint32, expiry time.Duration) error`               | Prepend a value to the key                                        |
| Cas            | ✅      | `Cas(ctx context.Context, key string, value []byte, flag uint32, expiry time.Duration, cas uint64) error`       | Compare and set a key-value pair to memcached                     |
| ----           | -----  | RETRIEVAL COMMANDS                                                                                                  | ---                                                               |
| Gets           | ✅      | `Gets(ctx context.Context, keys ...string) ([]*Item, error)`                                                        | Get a value by key from memcached with cas value                  |
| Get            | ✅      | `Get(ctx context.Context, key string) (*Item, error)`                                                               | Get a value by key from memcached                                 |
| GetAndTouch    | ✅      | `GetAndTouch(ctx context.Context, expiry time.Duration, key string) (*Item, error)`                                 | Get a value by key from memcached and touch the key's expire time |
| GetAndTouches  | ✅      | `GetAndTouches(ctx context.Context, expiry time.Duration, keys ...string) ([]*Item, error)`                         | Get a value by key from memcached and touch the key's expire time |
| -----          | -----  | OTHER COMMANDS                                                                                                      | ---                                                               |
| Delete         | ✅      | `Delete(ctx context.Context, key string) error`                                                                     | Delete a key-value pair from memcached                            |
| Incr           | ✅      | `Incr(ctx context.Context, key string, delta uint64) (uint64, error)`                                               | Increment a key's value                                           |
| Decr           | ✅      | `Decr(ctx context.Context, key string, delta uint64) (uint64, error)`                                               | Decrement a key's value                                           |
| Touch          | ✅      | `Touch(ctx context.Context, key string, expiry time.Duration) error`                                                | Touch a key's expire time                                         |
| Stats          | ✅      | `Stats(ctx context.Context) (*Statistic, error)`                                                                    | Get memcached server statistics                                   |
| MetaGet        | ✅      | `MetaGet(ctx context.Context, key []byte, options ...MetaGetOption) (*MetaItem, error)`                             | Get a key's meta information                                      |
| MetaSet        | ✅      | `MetaSet(ctx context.Context, key, value []byte, options ...MetaSetOption) (*MetaItem, error)`                      | Set a key's meta information                                      |
| MetaDelete     | ✅      | `MetaDelete(ctx context.Context, key []byte, options ...MetaDeleteOption) (*MetaItem, error)`                       | Delete a key's meta information                                   |
| MetaArithmetic | ✅      | `MetaArithmetic(ctx context.Context, key []byte, delta uint64, options ...MetaArithmeticOption) (*MetaItem, error)` | Arithmetic a key's meta information                               |
| MetaDebug      | ✅      | `MetaDebug(ctx context.Context, key []byte, options ...MetaDebugOption) (*MetaItemDebug, error)`                    | Debug a key's meta information                                    |
| MetaNoop       | ✅      | `MetaNoop(ctx context.Context) error`                                                                               | Noop a key's meta information                                     |
| Version        | ✅      | `Version(ctx context.Context) (string, error)`                                                                      | Get memcached server version                                      |
| FlushAll       | ✅      | `FlushAll(ctx context.Context) error`                                                                               | Flush all keys in memcached server                                |

### Development Guide

#### Prerequisites

- Go 1.27 or higher
- Python (for pre-commit hooks) or just `brew install pre-commit` on MacOS
- Docker (for running memcached in tests)

#### Setting up development environment

1. Clone the repository:
    ```bash
    git clone https://github.com/EyJunge1/gomemcached.git
    cd gomemcached
    ```
2. Install pre-commit hooks:
    ```bash
    pip install pre-commit
    # or MacOS
    # brew install pre-commit

    pre-commit install
    ```
3. Quality checks use the Makefile (`make lint` downloads golangci-lint via `go run`; no global install required).

#### Make targets

```bash
make lint      # golangci-lint v2 (gofumpt, gosec, unused, staticcheck)
make test      # go test -race
make coverage  # tests + coverage.html
make vuln      # govulncheck
make check     # lint + test + vuln
```

#### Running Tests

```bash
make test
# or with coverage
make coverage
```

#### Code Style

This project follows the standard Go code style guidelines and uses golangci-lint for additional checks, including **gosec**. Formatting is enforced by **gofumpt**. The configuration can be found in [.golangci.yml](./.golangci.yml).

Key points:
- Follow Go standard formatting (enforced by gofumpt)
- Ensure all code is properly tested
- Write clear commit messages
- Document public APIs
