package memcached

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Pipeline batches text commands: write every frame, then read every reply.
// Quiet mutations stay on MetaPipeline. Text noreply is not used.
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

// Pipeline starts a write-then-read text batch.
func (c *Client) Pipeline() *Pipeline {
	return &Pipeline{c: c}
}

func (p *Pipeline) addStorage(mode StoreMode, item *Item) *Pipeline {
	if p == nil || p.c == nil {
		return p
	}
	if item == nil {
		p.errs = append(p.errs, fmt.Errorf("nil item: %w", ErrInvalidArgument))
		return p
	}
	req := StoreRequest{Key: item.Key, Value: item.Value, Flags: item.Flags, Exp: item.Exp, CAS: item.CAS}
	command, err := storeCommand(mode)
	if err != nil {
		p.errs = append(p.errs, err)
		return p
	}
	key := p.c.nsBytes(req.Key)
	if err := p.c.checkKeyValue(key, req.Value); err != nil {
		p.errs = append(p.errs, err)
		return p
	}
	var request *request
	var resp *response
	if mode == StoreCAS {
		request, resp = buildCasCommand(key, req.Value, req.Flags, req.Exp, req.CAS, false)
	} else {
		if mode == StoreAppend || mode == StorePrepend {
			req.Flags = 0
			req.Exp = 0
		}
		request, resp = buildStorageCommand(command, key, req.Value, req.Flags, req.Exp, false)
	}
	resp.afterRecv = mapStorageReply
	p.ops = append(p.ops, pipeOp{req: request, resp: resp, key: key})
	return p
}

// Set queues a text set. Expiration comes from item.Exp.
func (p *Pipeline) Set(item *Item) *Pipeline { return p.addStorage(StoreSet, item) }

// Add queues a text add.
func (p *Pipeline) Add(item *Item) *Pipeline { return p.addStorage(StoreAdd, item) }

// Replace queues a text replace.
func (p *Pipeline) Replace(item *Item) *Pipeline { return p.addStorage(StoreReplace, item) }

// Append queues a text append.
func (p *Pipeline) Append(item *Item) *Pipeline { return p.addStorage(StoreAppend, item) }

// Prepend queues a text prepend.
func (p *Pipeline) Prepend(item *Item) *Pipeline { return p.addStorage(StorePrepend, item) }

// Cas queues a text cas.
func (p *Pipeline) Cas(item *Item) *Pipeline { return p.addStorage(StoreCAS, item) }

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

// Flush writes all queued frames, then reads all replies. Cluster nodes run in parallel.
func (p *Pipeline) Flush(ctx context.Context) error {
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

	groups := make(map[*Addr][]pipeOp, len(addrs))
	for _, op := range p.ops {
		addr, err := picker.Pick(addrs, p.c.hashKey(op.key))
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
	flushNode := func(addr *Addr, ops []pipeOp) {
		if err := p.flushAddr(ctx, addr, ops); err != nil {
			mu.Lock()
			errs = append(errs, err)
			mu.Unlock()
		}
	}
	if len(groups) == 1 {
		for addr, ops := range groups {
			flushNode(addr, ops)
		}
	} else {
		for addr, ops := range groups {
			wg.Go(func() { flushNode(addr, ops) })
		}
		wg.Wait()
	}
	return errors.Join(append(p.errs, errs...)...)
}

func (p *Pipeline) flushAddr(ctx context.Context, addr *Addr, ops []pipeOp) error {
	if isUDPNetwork(addr.Network) {
		return fmt.Errorf("udp pipeline: %w", ErrNotSupported)
	}
	cn, err := p.c.getConn(ctx, addr)
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
	if selectProximateDeadline(ctx, cn, p.c.options.writeTimeout, nowFunc, false) {
		defer func() { _ = cn.setWriteDeadline(zeroTime) }()
	}
	for _, op := range ops {
		if err = op.req.write(cn); err != nil {
			opErr = wrapTransport(fmt.Errorf("send failed: %w", err))
			p.c.noteFailure(addr)
			return opErr
		}
	}
	if err = cn.Flush(); err != nil {
		opErr = wrapTransport(fmt.Errorf("send failed: %w", err))
		p.c.noteFailure(addr)
		return opErr
	}
	_ = cn.setWriteDeadline(zeroTime)

	var joined []error
	for _, op := range ops {
		if op.resp != nil {
			op.resp.maxValueBytes = p.c.valueLimit()
			op.resp.readTimeout = p.c.options.readTimeout
			op.resp.readCtx = ctx
			op.resp.readConn = cn
		}
		if err = op.resp.recv(ctx, cn, p.c.options.readTimeout); err != nil {
			opErr = wrapRecvError(fmt.Errorf("recv failed: %w", err))
			if shouldDiscardPipelineConn(opErr) {
				p.c.noteFailure(addr)
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
					p.c.noteFailure(addr)
					return errors.Join(append(joined, err)...)
				}
				joined = append(joined, err)
			}
		}
	}
	p.c.noteSuccess(addr)
	return errors.Join(joined...)
}
