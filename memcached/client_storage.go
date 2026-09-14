package memcached

import (
	"context"
	"fmt"
	"time"
)

// StoreMode picks which text storage verb Store sends.
// The zero value is StoreSet.
type StoreMode uint8

const (
	// StoreSet overwrites regardless of prior existence. Zero Mode is set.
	StoreSet StoreMode = iota
	// StoreAdd stores only when the key is missing.
	StoreAdd
	// StoreReplace stores only when the key already exists.
	StoreReplace
	// StoreAppend appends to an existing value. Flags and Exptime are sent as 0 0.
	StoreAppend
	// StorePrepend prepends to an existing value. Flags and Exptime are sent as 0 0.
	StorePrepend
	// StoreCAS stores only when Item.CAS matches the server Unique.
	StoreCAS
)

// storeCommand maps StoreMode to the text storage verb.
func storeCommand(mode StoreMode) ([]byte, error) {
	switch mode {
	case StoreSet:
		return _SetBytes, nil
	case StoreAdd:
		return _AddBytes, nil
	case StoreReplace:
		return _ReplaceBytes, nil
	case StoreAppend:
		return _AppendBytes, nil
	case StorePrepend:
		return _PrependBytes, nil
	case StoreCAS:
		return _CasBytes, nil
	default:
		return nil, fmt.Errorf("store mode: %w", ErrInvalidArgument)
	}
}

// Store sends one text storage command. NOT_STORED, EXISTS, and NOT_FOUND are sentinels.
// Quiet adds noreply: the connection is discarded after send; CLIENT_ERROR is still returned.
func (c *Client) Store(ctx context.Context, item Item) error {
	command, err := storeCommand(item.Mode)
	if err != nil {
		return err
	}
	key, keyBox := c.nsKey(item.Key)
	if err := c.checkKeyValue(key, item.Value); err != nil {
		putBuilderBox(keyBox)
		return err
	}
	flags, exp := item.Flags, item.Exp
	if item.Mode == StoreAppend || item.Mode == StorePrepend {
		flags, exp = 0, 0
	}

	var request *request
	var resp *response
	if item.Mode == StoreCAS {
		request, resp = buildCasCommand(key, item.Value, flags, exp, item.CAS, item.Quiet)
	} else {
		request, resp = buildStorageCommand(command, key, item.Value, flags, exp, item.Quiet)
	}
	request.adoptKeyBox(keyBox)
	defer releaseReqAndResp(request, resp)

	if item.Quiet {
		return c.dispatchQuiet(ctx, request, resp)
	}
	resp.afterRecv = mapStorageReply
	if err := c.dispatchRequest(ctx, request, resp); err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	return nil
}

// dispatchQuiet writes a noreply command on the key's picked node.
func (c *Client) dispatchQuiet(ctx context.Context, req *request, resp *response) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	addrs, picker := c.snapshotCluster()
	addrs = c.liveAddrs(addrs)
	addr, err := picker.Pick(addrs, req.key)
	if err != nil {
		return fmt.Errorf("pick node failed: %w", err)
	}
	return c.sendQuietToAddr(ctx, addr, req, resp, true)
}

// quietFaultWait is how long a noreply write waits for CLIENT_ERROR or SERVER_ERROR.
const quietFaultWait = 25 * time.Millisecond

// connBuffered reports unread bytes on cn, or -1 if the conn does not expose a buffer.
func connBuffered(cn wireConn) int {
	type withBuf interface{ buffered() int }
	if b, ok := cn.(withBuf); ok {
		return b.buffered()
	}
	return -1
}

// readQuietFault briefly reads for CLIENT_ERROR or SERVER_ERROR after a noreply write.
func readQuietFault(ctx context.Context, cn wireConn) error {
	deadline := clock().Add(quietFaultWait)
	if ctx != nil {
		select {
		case <-ctx.Done():
			switch connBuffered(cn) {
			case 0:
				return nil
			case -1:
				deadline = clock().Add(time.Millisecond)
			default:
				deadline = clock().Add(time.Second)
			}
		default:
			if d, ok := ctx.Deadline(); ok && !d.IsZero() && d.Before(deadline) {
				deadline = d
			}
		}
	}
	_ = cn.setReadDeadline(deadline)
	defer clearReadDeadline(cn)
	line, err := cn.readLine('\n')
	if err != nil {
		return nil
	}
	if hit, ok := mapGlobalErrorLine(line, string(trimCRLF(line))); ok {
		return hit.Err
	}
	return nil
}

// sendQuietToAddr writes a noreply frame to node and optionally probes for a fault line.
func (c *Client) sendQuietToAddr(ctx context.Context, node *addr, req *request, resp *response, probeFault bool) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if node != nil && isUDPNetwork(node.Network) {
		return fmt.Errorf("udp %s: %w", req.cmd, ErrNotSupported)
	}
	cn, err := c.getConn(ctx, node)
	if err != nil {
		return wrapTransport(fmt.Errorf("alloc connection failed: %w", err))
	}
	c.enableUDPFraming(node, req, resp)
	stop := prepareOpIO(ctx, cn, c.rt.readTimeout, c.rt.writeTimeout)
	sendErr := req.sendReady(ctx, cn)
	stop()
	var fault error
	if sendErr == nil && probeFault {
		fault = readQuietFault(ctx, cn)
	}
	if p := cn.getConnPool(); p != nil {
		_ = p.discard(cn)
	} else {
		_ = cn.closeSilent()
	}
	if sendErr != nil {
		c.noteFailure(node)
		return wrapTransport(fmt.Errorf("send failed: %w", sendErr))
	}
	if fault != nil {
		return fault
	}
	c.noteSuccess(node)
	return nil
}
