package memcached

import (
	"context"
	"fmt"
)

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
	var buildErr error
	if item.Mode == StoreCAS {
		req, resp, buildErr = buildCASCommand(key, item.Value, flags, exp, item.CAS, item.Extra, false /* noReply */)
	} else {
		req, resp, buildErr = buildStorageCommand(command, key, item.Value, flags, exp, item.Extra, false /* noReply */)
	}
	if buildErr != nil {
		putBuilderBox(keyBox)
		return buildErr
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
	req, err := encodeGetOneCommand(command, exp, wired, opts.Extra)
	if err != nil {
		putBuilderBox(keyBox)
		return err
	}
	req.adoptKeyBox(keyBox)
	resp := buildRetrievalResponseOne(wired, dst, withCAS)
	defer releaseReqAndResp(req, resp)
	return s.execute(ctx, req, resp)
}

// Delete removes one key on the held connection. A miss is ErrNotFound.
// MutateOpts.Quiet returns ErrInvalidArgument (noreply is unsafe on a Session).
func (s *Session) Delete(ctx context.Context, key []byte, opts MutateOpts) error {
	if s == nil || s.c == nil {
		return ErrClosed
	}
	if opts.Quiet {
		return fmt.Errorf("session Quiet: %w", ErrInvalidArgument)
	}
	if err := s.begin(); err != nil {
		return err
	}
	defer s.running.Store(false)
	wired, keyBox, err := s.prepareKey(ctx, key, nil)
	if err != nil {
		return err
	}
	req, resp, err := buildDeleteCommand(wired, opts.Extra, false /* noReply */)
	if err != nil {
		putBuilderBox(keyBox)
		return err
	}
	req.adoptKeyBox(keyBox)
	resp.afterRecv = mapDeletedReply
	defer releaseReqAndResp(req, resp)
	return s.execute(ctx, req, resp)
}

// Touch changes one key's expiration on the held connection.
// MutateOpts.Quiet returns ErrInvalidArgument (noreply is unsafe on a Session).
func (s *Session) Touch(ctx context.Context, key []byte, expiry Exptime, opts MutateOpts) error {
	if s == nil || s.c == nil {
		return ErrClosed
	}
	if opts.Quiet {
		return fmt.Errorf("session Quiet: %w", ErrInvalidArgument)
	}
	if err := s.begin(); err != nil {
		return err
	}
	defer s.running.Store(false)
	wired, keyBox, err := s.prepareKey(ctx, key, nil)
	if err != nil {
		return err
	}
	req, resp, err := buildTouchCommand(wired, expiry, opts.Extra, false /* noReply */)
	if err != nil {
		putBuilderBox(keyBox)
		return err
	}
	req.adoptKeyBox(keyBox)
	resp.afterRecv = mapTouchedReply
	defer releaseReqAndResp(req, resp)
	return s.execute(ctx, req, resp)
}

// Incr increments one key and writes the updated counter into dst.
func (s *Session) Incr(ctx context.Context, key []byte, delta uint64, dst *uint64, opts ArithmeticOpts) error {
	if s == nil || s.c == nil {
		return ErrClosed
	}
	return s.arithmetic(ctx, "incr", key, delta, dst, opts)
}

// Decr decrements one key and writes the updated counter into dst.
func (s *Session) Decr(ctx context.Context, key []byte, delta uint64, dst *uint64, opts ArithmeticOpts) error {
	if s == nil || s.c == nil {
		return ErrClosed
	}
	return s.arithmetic(ctx, "decr", key, delta, dst, opts)
}

func (s *Session) arithmetic(
	ctx context.Context, cmd string, key []byte, delta uint64, dst *uint64, opts ArithmeticOpts,
) error {
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
	req, resp, err := buildArithmeticCommand(cmd, wired, delta, opts.Extra, false /* noReply */)
	if err != nil {
		putBuilderBox(keyBox)
		return err
	}
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
			return fmt.Errorf("pick node: %w", err)
		}
	}
	if node.id() != s.node.id() {
		return fmt.Errorf("key routes to another session node: %w", ErrInvalidArgument)
	}
	return nil
}

func (s *Session) execute(ctx context.Context, req *request, resp *response) error {
	h := &connHolder{cn: s.cn}
	err := s.c.sendOnConn(ctx, s.node, h, req, resp)
	s.cn = h.cn
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
				return fmt.Errorf("pick node: %w", err)
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
