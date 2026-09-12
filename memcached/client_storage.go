package memcached

import (
	"context"
	"fmt"
	"time"
)

// StoreMode picks which text storage verb Store sends.
// The zero value is treated as StoreSet (unset → default).
type StoreMode uint8

const (
	// StoreSet overwrites regardless of prior existence.
	StoreSet StoreMode = iota + 1
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

// Store sends one text storage command. NOT_STORED, EXISTS, and NOT_FOUND are sentinels.
// Quiet adds noreply: the connection is discarded after send; CLIENT_ERROR and
// SERVER_ERROR are still returned when the server already replied.
func (c *Client) Store(ctx context.Context, item Item) error {
	return c.storeItem(ctx, nil, item)
}

// storeOnAddr is Store routed to a fixed node.
func (c *Client) storeOnAddr(ctx context.Context, node *addr, item Item) error {
	return c.storeItem(ctx, node, item)
}

// storeItem sends one storage command. node nil picks by key; non-nil pins the node.
func (c *Client) storeItem(ctx context.Context, node *addr, item Item) (err error) {
	command, cmdErr := storeCommand(item.Mode)
	if cmdErr != nil {
		return cmdErr
	}
	op := string(command)
	obs := c.observer()
	var start time.Time
	if obs != nil {
		start = time.Now()
		defer func() {
			c.observeEvent(ctx, OpEvent{
				Op:           op,
				Server:       serverFromAddr(node),
				Duration:     time.Since(start),
				Err:          err,
				Keys:         1,
				RequestBytes: int64(len(item.Value)),
				Key:          item.Key,
			})
		}()
	}
	key, keyBox := c.nsKey(item.Key)
	if err = c.checkKeyValue(key, item.Value); err != nil {
		putBuilderBox(keyBox)
		return err
	}
	flags, exp := item.Flags, item.Exp
	if item.Mode == StoreAppend || item.Mode == StorePrepend {
		flags, exp = 0, 0
	}

	var req *request
	var resp *response
	var buildErr error
	if item.Mode == StoreCAS {
		req, resp, buildErr = buildCASCommand(key, item.Value, flags, exp, item.CAS, item.Extra, item.Quiet)
	} else {
		req, resp, buildErr = buildStorageCommand(command, key, item.Value, flags, exp, item.Extra, item.Quiet)
	}
	if buildErr != nil {
		putBuilderBox(keyBox)
		return buildErr
	}
	req.adoptKeyBox(keyBox)
	defer releaseReqAndResp(req, resp)

	if node == nil {
		node, err = c.pickNode(key)
		if err != nil {
			return err
		}
	}

	skip := ctx
	if obs != nil {
		skip = withObserveSkip(ctx)
	}
	if item.Quiet {
		return c.sendQuietToAddr(skip, node, req, resp, true /* probeFault */)
	}
	resp.afterRecv = mapStorageReply
	if err = c.dispatchToAddr(skip, node, req, resp); err != nil {
		return fmt.Errorf("request: %w", err)
	}
	return nil
}

// dispatchQuiet writes a noreply command on the key's picked node.
func (c *Client) dispatchQuiet(ctx context.Context, req *request, resp *response) (err error) {
	obs := c.observer()
	var start time.Time
	var node *addr
	if obs != nil && !observeSkipped(ctx) {
		start = time.Now()
		defer func() {
			c.observeEvent(ctx, OpEvent{
				Op:           opName(req),
				Server:       serverFromAddr(node),
				Duration:     time.Since(start),
				Err:          err,
				Keys:         1,
				RequestBytes: requestWireBytes(req),
			})
		}()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	addrs, picker := c.snapshotCluster()
	addrs = c.liveAddrs(addrs)
	node, err = picker.Pick(addrs, req.key)
	if err != nil {
		return fmt.Errorf("pick node: %w", err)
	}
	return c.sendQuietToAddr(ctx, node, req, resp, true /* probeFault */)
}

// sendQuietToAddr writes a noreply frame to node and optionally probes for a fault line.
func (c *Client) sendQuietToAddr(ctx context.Context, node *addr, req *request, resp *response, probeFault bool) error {
	if req == nil || resp == nil {
		return fmt.Errorf("quiet request: %w", ErrInvalidArgument)
	}
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
		return wrapTransport(fmt.Errorf("alloc connection: %w", err))
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
		return wrapTransport(fmt.Errorf("send: %w", sendErr))
	}
	if fault != nil {
		return fault
	}
	c.noteSuccess(node)
	return nil
}

// storeCommand maps StoreMode to the text storage verb.
func storeCommand(mode StoreMode) ([]byte, error) {
	switch mode {
	case 0, StoreSet:
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

// _quietFaultWait is how long a noreply write waits for CLIENT_ERROR or SERVER_ERROR.
const _quietFaultWait = 25 * time.Millisecond

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
	deadline := time.Now().Add(_quietFaultWait)
	if ctx != nil {
		select {
		case <-ctx.Done():
			switch connBuffered(cn) {
			case 0:
				return nil
			case -1:
				deadline = time.Now().Add(time.Millisecond)
			default:
				deadline = time.Now().Add(time.Second)
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
