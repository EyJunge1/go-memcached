package memcached

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
)

// Session exclusively holds one pooled stream connection for sequential
// operations on a single routed node. It must not be used concurrently.
type Session struct {
	c       *Client
	node    *addr
	cn      wireConn
	version uint64

	running atomic.Bool
	closed  atomic.Bool
}

// Session checks out a connection for key's current node. Every later key must
// route to that node. Close returns a healthy connection to its pool. UDP does
// not support stream sessions.
func (c *Client) Session(ctx context.Context, key []byte) (*Session, error) {
	if c == nil || c.closed.Load() {
		return nil, ErrClosed
	}
	wired, box := c.nsKey(key)
	defer putBuilderBox(box)
	if err := validateKeyAndValue(wired, nil); err != nil {
		return nil, err
	}
	node, err := c.pickNode(wired)
	if err != nil {
		return nil, err
	}
	if isUDPNetwork(node.Network) {
		return nil, fmt.Errorf("udp session: %w", ErrNotSupported)
	}
	cn, err := c.getConn(ctx, node)
	if err != nil {
		c.noteFailure(node)
		return nil, wrapTransport(fmt.Errorf("alloc connection failed: %w", err))
	}
	return &Session{
		c: c, node: node, cn: cn, version: c.clusterVersion.Load(),
	}, nil
}

// Server returns the node held by the session.
func (s *Session) Server() Server {
	if s == nil || s.node == nil {
		return Server{}
	}
	return addrToServer(s.node)
}

// Run executes and releases a Pipeline on the held connection. Every queued
// key must route to this Session's node.
func (s *Session) Run(ctx context.Context, p *Pipeline) error {
	if p == nil {
		return fmt.Errorf("nil pipeline: %w", ErrInvalidArgument)
	}
	defer p.release()
	if s == nil || s.c == nil {
		failNotExecuted(p.ops, ErrClosed)
		return ErrClosed
	}
	if p.c == nil || p.c != s.c || p.prepared {
		return fmt.Errorf("pipeline belongs to another execution mode: %w", ErrInvalidArgument)
	}
	if err := s.begin(); err != nil {
		failNotExecuted(p.ops, err)
		return err
	}
	defer s.running.Store(false)
	if err := ctx.Err(); err != nil {
		failNotExecuted(p.ops, err)
		return err
	}
	if err := s.validateOps(p.ops); err != nil {
		failNotExecuted(p.ops, err)
		return errors.Join(append(p.errs, err)...)
	}
	if len(p.ops) == 0 {
		return errors.Join(p.errs...)
	}
	err := p.runConn(ctx, s.node, s.cn, p.ops)
	s.afterRun(err)
	return errors.Join(append(p.errs, err)...)
}

// RunPrepared executes a single-node PreparedBatch on the held connection,
// avoiding connection checkout, validation, encoding, and routing in the
// steady state.
func (s *Session) RunPrepared(ctx context.Context, b *PreparedBatch) error {
	return s.runPrepared(ctx, b, nil)
}

// RunPreparedValues executes a PreparedBatch with borrowed replacement
// storage values while retaining this session's dedicated connection.
func (s *Session) RunPreparedValues(ctx context.Context, b *PreparedBatch, values [][]byte) error {
	if b == nil || b.p == nil || b.closed.Load() {
		return fmt.Errorf("invalid prepared batch: %w", ErrInvalidArgument)
	}
	if len(values) != len(b.valueOps) {
		return fmt.Errorf("got %d values for %d storage operations: %w",
			len(values), len(b.valueOps), ErrInvalidArgument)
	}
	for i, opIndex := range b.valueOps {
		// #nosec G602 -- equal lengths are checked above and valueOps is internal.
		if err := b.c.checkKeyValue(b.p.ops[opIndex].key, values[i]); err != nil {
			return err
		}
	}
	return s.runPrepared(ctx, b, values)
}

func (s *Session) runPrepared(ctx context.Context, b *PreparedBatch, values [][]byte) error {
	if s == nil || s.c == nil {
		return ErrClosed
	}
	if b == nil || b.c != s.c || b.p == nil || b.closed.Load() {
		return fmt.Errorf("prepared batch belongs to another client: %w", ErrInvalidArgument)
	}
	if err := s.begin(); err != nil {
		return err
	}
	defer s.running.Store(false)
	if !b.running.CompareAndSwap(false, true) {
		return fmt.Errorf("prepared batch is already running: %w", ErrInvalidArgument)
	}
	defer b.running.Store(false)
	if values != nil {
		for i, opIndex := range b.valueOps {
			b.p.ops[opIndex].req.body = values[i]
		}
		defer b.restoreDefaultValues()
	}
	if err := b.updateValueLengths(); err != nil {
		failNotExecuted(b.p.ops, err)
		return err
	}
	if err := ctx.Err(); err != nil {
		failNotExecuted(b.p.ops, err)
		return err
	}
	resetPreparedBatch(b.p.ops)
	if b.clusterVersion != b.c.clusterVersion.Load() ||
		b.healthVersion != b.c.healthVersion.Load() {
		if err := b.refreshRoutes(); err != nil {
			failNotExecuted(b.p.ops, err)
			return err
		}
	}
	if len(b.groups) != 1 || b.groups[0].node.id() != s.node.id() {
		err := fmt.Errorf("prepared batch does not belong to session node: %w", ErrInvalidArgument)
		failNotExecuted(b.p.ops, err)
		return err
	}
	group := &b.groups[0]
	var err error
	if group.frames == nil {
		err = b.p.runConn(ctx, s.node, s.cn, group.ops)
	} else {
		err = b.p.runFrames(ctx, s.node, s.cn, group.ops, group.frames.frames)
	}
	s.afterRun(err)
	return err
}

// Store executes one storage command on the held connection. The key must
// route to the Session's node. Item.Quiet returns ErrInvalidArgument because
// noreply would make a reusable Session connection unsafe.
func (s *Session) Store(ctx context.Context, item Item) error {
	if s == nil || s.c == nil {
		return ErrClosed
	}
	if item.Quiet {
		return fmt.Errorf("session Quiet: %w", ErrInvalidArgument)
	}
	command, err := storeCommand(item.Mode)
	if err != nil {
		return err
	}
	if err = s.begin(); err != nil {
		return err
	}
	defer s.running.Store(false)
	key, keyBox, err := s.prepareKey(ctx, item.Key, item.Value)
	if err != nil {
		return err
	}
	flags, exp := item.Flags, item.Exp
	if item.Mode == StoreAppend || item.Mode == StorePrepend {
		flags, exp = 0, 0
	}
	var req *request
	var resp *response
	if item.Mode == StoreCAS {
		req, resp = buildCasCommand(key, item.Value, flags, exp, item.CAS, false)
	} else {
		req, resp = buildStorageCommand(command, key, item.Value, flags, exp, false)
	}
	req.adoptKeyBox(keyBox)
	resp.afterRecv = mapStorageReply
	defer releaseReqAndResp(req, resp)
	return s.execute(ctx, req, resp)
}

// GetOne retrieves one key on the held connection. A miss leaves dst.Found
// false and is not ErrNotFound. The key must route to the Session's node.
func (s *Session) GetOne(ctx context.Context, key []byte, dst *GetResult, opts GetOpts) error {
	if s == nil || s.c == nil {
		return ErrClosed
	}
	if dst == nil {
		return fmt.Errorf("get dst: %w", ErrInvalidArgument)
	}
	if err := s.begin(); err != nil {
		return err
	}
	defer s.running.Store(false)
	resetGetResult(dst)
	wired, keyBox, err := s.prepareKey(ctx, key, nil)
	if err != nil {
		return err
	}
	command, withCAS := getCommand(opts)
	var exp Exptime
	if opts.Touch != nil {
		exp = *opts.Touch
	}
	req := encodeGetOneCommand(command, exp, wired)
	req.adoptKeyBox(keyBox)
	resp := buildRetrievalResponseOne(wired, dst, withCAS)
	defer releaseReqAndResp(req, resp)
	return s.execute(ctx, req, resp)
}

// Delete removes one key on the held connection. A miss is ErrNotFound.
func (s *Session) Delete(ctx context.Context, key []byte) error {
	if s == nil || s.c == nil {
		return ErrClosed
	}
	if err := s.begin(); err != nil {
		return err
	}
	defer s.running.Store(false)
	wired, keyBox, err := s.prepareKey(ctx, key, nil)
	if err != nil {
		return err
	}
	req, resp := buildDeleteCommand(wired, false)
	req.adoptKeyBox(keyBox)
	resp.afterRecv = mapDeletedReply
	defer releaseReqAndResp(req, resp)
	return s.execute(ctx, req, resp)
}

// Touch changes one key's expiration on the held connection.
func (s *Session) Touch(ctx context.Context, key []byte, expiry Exptime) error {
	if s == nil || s.c == nil {
		return ErrClosed
	}
	if err := s.begin(); err != nil {
		return err
	}
	defer s.running.Store(false)
	wired, keyBox, err := s.prepareKey(ctx, key, nil)
	if err != nil {
		return err
	}
	req, resp := buildTouchCommand(wired, expiry, false)
	req.adoptKeyBox(keyBox)
	resp.afterRecv = mapTouchedReply
	defer releaseReqAndResp(req, resp)
	return s.execute(ctx, req, resp)
}

// Incr increments one key and writes the updated counter into dst.
func (s *Session) Incr(ctx context.Context, key []byte, delta uint64, dst *uint64) error {
	if s == nil || s.c == nil {
		return ErrClosed
	}
	return s.arithmetic(ctx, "incr", key, delta, dst)
}

// Decr decrements one key and writes the updated counter into dst.
func (s *Session) Decr(ctx context.Context, key []byte, delta uint64, dst *uint64) error {
	if s == nil || s.c == nil {
		return ErrClosed
	}
	return s.arithmetic(ctx, "decr", key, delta, dst)
}

func (s *Session) arithmetic(ctx context.Context, cmd string, key []byte, delta uint64, dst *uint64) error {
	if dst == nil {
		return fmt.Errorf("arithmetic dst: %w", ErrInvalidArgument)
	}
	*dst = 0
	if err := s.begin(); err != nil {
		return err
	}
	defer s.running.Store(false)
	wired, keyBox, err := s.prepareKey(ctx, key, nil)
	if err != nil {
		return err
	}
	req, resp := buildArithmeticCommand(cmd, wired, delta, false)
	req.adoptKeyBox(keyBox)
	resp.afterRecv = mapArithmeticReply
	defer releaseReqAndResp(req, resp)
	if err = s.execute(ctx, req, resp); err != nil {
		return err
	}
	*dst = resp.arithValue
	return nil
}

func (s *Session) prepareKey(ctx context.Context, key, value []byte) ([]byte, *[]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	wired, keyBox := s.c.nsKey(key)
	if err := s.c.checkKeyValue(wired, value); err != nil {
		putBuilderBox(keyBox)
		return nil, nil, err
	}
	if err := s.validateKey(wired); err != nil {
		putBuilderBox(keyBox)
		return nil, nil, err
	}
	return wired, keyBox, nil
}

func (s *Session) validateKey(key []byte) error {
	addrs, picker := s.c.snapshotCluster()
	if picker == nil || len(addrs) == 0 {
		return fmt.Errorf("no servers configured: %w", ErrInvalidAddress)
	}
	addrs = s.c.liveAddrs(addrs)
	node := addrs[0]
	if len(addrs) > 1 {
		var err error
		node, err = picker.Pick(addrs, key)
		if err != nil {
			return fmt.Errorf("pick node failed: %w", err)
		}
	}
	if node.id() != s.node.id() {
		return fmt.Errorf("key routes to another session node: %w", ErrInvalidArgument)
	}
	return nil
}

func (s *Session) execute(ctx context.Context, req *request, resp *response) error {
	err := s.c.sendOnConn(ctx, s.node, s.cn, req, resp)
	if err == nil && resp.afterRecv != nil {
		err = wrapRecvError(resp.afterRecv(resp))
	}
	if shouldDiscardConn(err) {
		s.c.noteFailure(s.node)
	} else {
		s.c.noteSuccess(s.node)
	}
	s.afterRun(err)
	return err
}

// Close returns a healthy connection to its pool. An operation automatically
// discards the connection and closes the Session when protocol position becomes
// indeterminate.
func (s *Session) Close() error {
	if s == nil || s.closed.Load() {
		return nil
	}
	if !s.running.CompareAndSwap(false, true) {
		return fmt.Errorf("session is running: %w", ErrInvalidArgument)
	}
	defer s.running.Store(false)
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	if s.cn == nil {
		return nil
	}
	err := s.cn.release()
	s.cn = nil
	return err
}

func (s *Session) begin() error {
	if s == nil || s.c == nil || s.cn == nil || s.closed.Load() || s.c.closed.Load() {
		return ErrClosed
	}
	if s.version != s.c.clusterVersion.Load() {
		return fmt.Errorf("cluster changed while session was held: %w", ErrInvalidAddress)
	}
	if !s.running.CompareAndSwap(false, true) {
		return fmt.Errorf("session is already running: %w", ErrInvalidArgument)
	}
	return nil
}

func (s *Session) validateOps(ops []pipeOp) error {
	addrs, picker := s.c.snapshotCluster()
	if picker == nil || len(addrs) == 0 {
		return fmt.Errorf("no servers configured: %w", ErrInvalidAddress)
	}
	addrs = s.c.liveAddrs(addrs)
	for i := range ops {
		node := addrs[0]
		if len(addrs) > 1 {
			var err error
			node, err = picker.Pick(addrs, ops[i].key)
			if err != nil {
				return fmt.Errorf("pick node failed: %w", err)
			}
		}
		if node.id() != s.node.id() {
			return fmt.Errorf("pipeline key routes to another session node: %w", ErrInvalidArgument)
		}
	}
	return nil
}

func (s *Session) afterRun(err error) {
	if !shouldDiscardPipelineConn(err) || s.cn == nil {
		return
	}
	if pool := s.cn.getConnPool(); pool != nil {
		_ = pool.discard(s.cn)
	} else {
		_ = s.cn.closeSilent()
	}
	s.cn = nil
	s.closed.Store(true)
}
