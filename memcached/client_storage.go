package memcached

import (
	"context"
	"fmt"
	"time"
)

// ItemWriteOption customizes a single storage/mutation call.
type ItemWriteOption func(*itemWriteOpts)

type itemWriteOpts struct {
	noReply bool
}

// NoReply omits the server response for this call. Overrides the client-wide
// WithNoReply default when passed; without it the client default is used.
func NoReply() ItemWriteOption {
	return func(o *itemWriteOpts) { o.noReply = true }
}

func (c *Client) resolveNoReply(opts []ItemWriteOption) bool {
	o := itemWriteOpts{noReply: c.options.noReply}
	for _, fn := range opts {
		fn(&o)
	}
	return o.noReply
}

func (c *Client) storageCommand(ctx context.Context, command, key string, value []byte, flag uint32, expiry time.Duration, opts ...ItemWriteOption) error {
	key = c.nsString(key)
	if err := validateKeyAndValue([]byte(key), value); err != nil {
		return err
	}

	req, resp := buildStorageCommand(command, key, value, flag, expiry, c.resolveNoReply(opts))
	defer releaseReqAndResp(req, resp)

	resp.afterRecv = mapStorageReply
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return fmt.Errorf("request failed: %w", err)
	}

	return nil
}

// Set stores the given key-value pair.
func (c *Client) Set(ctx context.Context, key string, value []byte, flag uint32, expiry time.Duration, opts ...ItemWriteOption) error {
	return c.storageCommand(ctx, "set", key, value, flag, expiry, opts...)
}

// Add stores the pair only if the key does not exist.
func (c *Client) Add(ctx context.Context, key string, value []byte, flag uint32, expiry time.Duration, opts ...ItemWriteOption) error {
	return c.storageCommand(ctx, "add", key, value, flag, expiry, opts...)
}

// Replace updates an existing item.
func (c *Client) Replace(ctx context.Context, key string, value []byte, flag uint32, expiry time.Duration, opts ...ItemWriteOption) error {
	return c.storageCommand(ctx, "replace", key, value, flag, expiry, opts...)
}

// Append appends value to an existing item.
func (c *Client) Append(ctx context.Context, key string, value []byte, flag uint32, expiry time.Duration, opts ...ItemWriteOption) error {
	return c.storageCommand(ctx, "append", key, value, flag, expiry, opts...)
}

// Prepend prepends value to an existing item.
func (c *Client) Prepend(ctx context.Context, key string, value []byte, flag uint32, expiry time.Duration, opts ...ItemWriteOption) error {
	return c.storageCommand(ctx, "prepend", key, value, flag, expiry, opts...)
}

// Cas updates an existing item using check-and-set.
func (c *Client) Cas(ctx context.Context, key string, value []byte, flag uint32, expiry time.Duration, cas uint64, opts ...ItemWriteOption) error {
	key = c.nsString(key)
	if err := validateKeyAndValue([]byte(key), value); err != nil {
		return err
	}

	req, resp := buildCasCommand(key, value, flag, expiry, cas, c.resolveNoReply(opts))
	defer releaseReqAndResp(req, resp)

	resp.afterRecv = mapStorageReply
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return fmt.Errorf("request failed: %w", err)
	}

	return nil
}
