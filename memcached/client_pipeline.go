package memcached

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Pipeline batches text commands: write every frame, then read every reply.
// Quiet mutations stay on Meta().Pipeline. Text noreply is not used.
type Pipeline struct {
	c    *Client
	ops  []pipeOp
	errs []error
}

type pipeOp struct {
	req  *request
	resp *response
	key  []byte
}

// Pipeline starts a write-then-read text batch. Meta commands use Meta().Pipeline.
func (c *Client) Pipeline() *Pipeline {
	return &Pipeline{c: c}
}

func (p *Pipeline) Store(item Item) *Pipeline {
	if p == nil || p.c == nil {
		return p
	}
	if item.Quiet {
		p.errs = append(p.errs, fmt.Errorf("pipeline Quiet: %w", ErrInvalidArgument))
		return p
	}
	command, err := storeCommand(item.Mode)
	if err != nil {
		p.errs = append(p.errs, err)
		return p
	}
	key := p.c.nsBytes(item.Key)
	if err := p.c.checkKeyValue(key, item.Value); err != nil {
		p.errs = append(p.errs, err)
		return p
	}
	flags, exp := item.Flags, item.Exp
	if item.Mode == StoreAppend || item.Mode == StorePrepend {
		flags, exp = 0, 0
	}
	var request *request
	var resp *response
	if item.Mode == StoreCAS {
		request, resp = buildCasCommand(key, item.Value, flags, exp, item.CAS, false)
	} else {
		request, resp = buildStorageCommand(command, key, item.Value, flags, exp, false)
	}
	resp.afterRecv = mapStorageReply
	p.ops = append(p.ops, pipeOp{req: request, resp: resp, key: key})
	return p
}

// Delete queues a text delete.
func (p *Pipeline) Delete(key []byte) *Pipeline {
	if p == nil || p.c == nil {
		return p
	}
	key = p.c.nsBytes(key)
	if err := validateKeyAndValue(key, nil); err != nil {
		p.errs = append(p.errs, err)
		return p
	}
	req, resp := buildDeleteCommand(key, false)
	resp.afterRecv = expectLine(_DeletedCRLFBytes)
	p.ops = append(p.ops, pipeOp{req: req, resp: resp, key: key})
	return p
}

// Touch queues a text touch.
func (p *Pipeline) Touch(key []byte, expiry Exptime) *Pipeline {
	if p == nil || p.c == nil {
		return p
	}
	key = p.c.nsBytes(key)
	if err := validateKeyAndValue(key, nil); err != nil {
		p.errs = append(p.errs, err)
		return p
	}
	req, resp := buildTouchCommand(key, expiry, false)
	resp.afterRecv = expectLine(_TouchedCRLFBytes)
	p.ops = append(p.ops, pipeOp{req: req, resp: resp, key: key})
	return p
}

func (p *Pipeline) release() {
	for _, op := range p.ops {
		releaseReqAndResp(op.req, op.resp)
	}
	p.ops = nil
}

func shouldDiscardPipelineConn(err error) bool {
	return shouldDiscardConn(err)
}

// Run writes all queued frames, then reads all replies. Cluster nodes run in parallel.
func (p *Pipeline) Run(ctx context.Context) error {
	if p == nil || p.c == nil {
		return fmt.Errorf("nil pipeline: %w", ErrInvalidArgument)
	}
	defer p.release()
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(p.ops) == 0 {
		return errors.Join(p.errs...)
	}

	addrs, picker := p.c.snapshotCluster()
	if picker == nil || len(addrs) == 0 {
		return fmt.Errorf("no servers configured: %w", ErrInvalidAddress)
	}
	addrs = p.c.liveAddrs(addrs)

	groups := make(map[*addr][]pipeOp, len(addrs))
	for _, op := range p.ops {
		addr, err := picker.Pick(addrs, op.key)
		if err != nil {
			p.errs = append(p.errs, fmt.Errorf("pick node failed: %w", err))
			continue
		}
		groups[addr] = append(groups[addr], op)
	}

	var (
		mu   sync.Mutex
		errs []error
		wg   sync.WaitGroup
	)
	runNode := func(node *addr, ops []pipeOp) {
		if err := p.runAddr(ctx, node, ops); err != nil {
			mu.Lock()
			errs = append(errs, err)
			mu.Unlock()
		}
	}
	if len(groups) == 1 {
		for addr, ops := range groups {
			runNode(addr, ops)
		}
	} else {
		for addr, ops := range groups {
			wg.Go(func() { runNode(addr, ops) })
		}
		wg.Wait()
	}
	return errors.Join(append(p.errs, errs...)...)
}

func (p *Pipeline) runAddr(ctx context.Context, node *addr, ops []pipeOp) error {
	if isUDPNetwork(node.Network) {
		return fmt.Errorf("udp pipeline: %w", ErrNotSupported)
	}
	cn, err := p.c.getConn(ctx, node)
	if err != nil {
		return wrapTransport(fmt.Errorf("alloc connection failed: %w", err))
	}
	var opErr error
	defer func() {
		if shouldDiscardPipelineConn(opErr) {
			if pool := cn.getConnPool(); pool != nil {
				_ = pool.discard(cn)
				return
			}
			_ = cn.Close()
			return
		}
		_ = cn.release()
	}()

	stop := watchContextCancel(ctx, cn)
	defer stop()
	if selectProximateDeadline(ctx, cn, p.c.rt.writeTimeout, clock, false) {
		defer func() { _ = cn.setWriteDeadline(zeroTime) }()
	}
	for _, op := range ops {
		if err = op.req.write(cn); err != nil {
			opErr = wrapTransport(fmt.Errorf("send failed: %w", err))
			p.c.noteFailure(node)
			return opErr
		}
	}
	if err = cn.Flush(); err != nil {
		opErr = wrapTransport(fmt.Errorf("send failed: %w", err))
		p.c.noteFailure(node)
		return opErr
	}
	_ = cn.setWriteDeadline(zeroTime)

	var joined []error
	for _, op := range ops {
		if op.resp != nil {
			op.resp.maxValueBytes = p.c.valueLimit()
			op.resp.readTimeout = p.c.rt.readTimeout
			op.resp.readCtx = ctx
			op.resp.readConn = cn
		}
		if err = op.resp.recv(ctx, cn, p.c.rt.readTimeout); err != nil {
			opErr = wrapRecvError(fmt.Errorf("recv failed: %w", err))
			if shouldDiscardPipelineConn(opErr) {
				p.c.noteFailure(node)
				return errors.Join(append(joined, opErr)...)
			}
			joined = append(joined, opErr)
			opErr = nil
			continue
		}
		if op.resp.afterRecv != nil {
			if err = wrapRecvError(op.resp.afterRecv(op.resp)); err != nil {
				if shouldDiscardPipelineConn(err) {
					opErr = err
					p.c.noteFailure(node)
					return errors.Join(append(joined, err)...)
				}
				joined = append(joined, err)
			}
		}
	}
	p.c.noteSuccess(node)
	return errors.Join(joined...)
}
