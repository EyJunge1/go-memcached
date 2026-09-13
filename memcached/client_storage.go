package memcached

import (
	"context"
	"fmt"
)

// StoreMode selects the text storage command.
type StoreMode uint8

const (
	StoreSet StoreMode = iota
	StoreAdd
	StoreReplace
	StoreAppend
	StorePrepend
	StoreCAS
)

// StoreRequest is one storage mutation.
type StoreRequest struct {
	Key   []byte
	Value []byte
	Flags uint32
	Exp   Exptime
	CAS   uint64 // StoreCAS only
	Quiet bool
}

func storeCommand(mode StoreMode) (string, error) {
	switch mode {
	case StoreSet:
		return "set", nil
	case StoreAdd:
		return "add", nil
	case StoreReplace:
		return "replace", nil
	case StoreAppend:
		return "append", nil
	case StorePrepend:
		return "prepend", nil
	case StoreCAS:
		return "cas", nil
	default:
		return "", fmt.Errorf("store mode: %w", ErrInvalidArgument)
	}
}

// Store issues one text storage command.
// NOT_STORED, EXISTS, and NOT_FOUND are returned as sentinels.
// Quiet sends noreply: success means the write was sent, not that the
// server accepted the item. The connection is discarded.
func (c *Client) Store(ctx context.Context, mode StoreMode, req StoreRequest) error {
	command, err := storeCommand(mode)
	if err != nil {
		return err
	}
	key := c.nsBytes(req.Key)
	if err := c.checkKeyValue(key, req.Value); err != nil {
		return err
	}
	flags, exp := req.Flags, req.Exp
	if mode == StoreAppend || mode == StorePrepend {
		flags, exp = 0, 0
	}

	var request *request
	var resp *response
	if mode == StoreCAS {
		request, resp = buildCasCommand(key, req.Value, flags, exp, req.CAS, req.Quiet)
	} else {
		request, resp = buildStorageCommand(command, key, req.Value, flags, exp, req.Quiet)
	}
	defer releaseReqAndResp(request, resp)

	if req.Quiet {
		return c.dispatchQuiet(ctx, request, resp)
	}
	resp.afterRecv = mapStorageReply
	if err := c.dispatchRequest(ctx, request, resp); err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	return nil
}

func (c *Client) dispatchQuiet(ctx context.Context, req *request, resp *response) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	addrs, picker := c.snapshotCluster()
	addrs = c.liveAddrs(addrs)
	addr, err := picker.Pick(addrs, c.hashKey(req.key))
	if err != nil {
		return fmt.Errorf("pick node failed: %w", err)
	}
	return c.sendQuietToAddr(ctx, addr, req, resp)
}

func (c *Client) sendQuietToAddr(ctx context.Context, addr *Addr, req *request, resp *response) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if addr != nil && isUDPNetwork(addr.Network) {
		return fmt.Errorf("udp %s: %w", req.cmd, ErrNotSupported)
	}
	cn, err := c.getConn(ctx, addr)
	if err != nil {
		return wrapTransport(fmt.Errorf("alloc connection failed: %w", err))
	}
	c.enableUDPFraming(addr, req, resp)
	sendErr := req.send(ctx, cn, c.options.writeTimeout)
	if p := cn.getConnPool(); p != nil {
		_ = p.discard(cn)
	} else {
		_ = cn.Close()
	}
	if sendErr != nil {
		c.noteFailure(addr)
		return wrapTransport(fmt.Errorf("send failed: %w", sendErr))
	}
	c.noteSuccess(addr)
	return nil
}

// Set stores the given key-value pair.
func (c *Client) Set(ctx context.Context, key, value []byte, flag uint32, expiry Exptime) error {
	return c.Store(ctx, StoreSet, StoreRequest{Key: key, Value: value, Flags: flag, Exp: expiry})
}

// Add stores the pair only if the key does not exist.
func (c *Client) Add(ctx context.Context, key, value []byte, flag uint32, expiry Exptime) error {
	return c.Store(ctx, StoreAdd, StoreRequest{Key: key, Value: value, Flags: flag, Exp: expiry})
}

// Replace updates an existing item.
func (c *Client) Replace(ctx context.Context, key, value []byte, flag uint32, expiry Exptime) error {
	return c.Store(ctx, StoreReplace, StoreRequest{Key: key, Value: value, Flags: flag, Exp: expiry})
}

// Append appends value to an existing item.
func (c *Client) Append(ctx context.Context, key, value []byte) error {
	return c.Store(ctx, StoreAppend, StoreRequest{Key: key, Value: value})
}

// Prepend prepends value to an existing item.
func (c *Client) Prepend(ctx context.Context, key, value []byte) error {
	return c.Store(ctx, StorePrepend, StoreRequest{Key: key, Value: value})
}

// Cas updates an existing item using check-and-set.
func (c *Client) Cas(ctx context.Context, key, value []byte, flag uint32, expiry Exptime, cas uint64) error {
	return c.Store(ctx, StoreCAS, StoreRequest{Key: key, Value: value, Flags: flag, Exp: expiry, CAS: cas})
}
