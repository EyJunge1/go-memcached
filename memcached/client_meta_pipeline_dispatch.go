package memcached

import (
	"bytes"
	"context"
	"errors"
)

func (c *Client) metaPipelineDispatch(ctx context.Context, node *addr, group []preparedMetaOp, out []MetaResult) error {
	if len(group) == 0 {
		return nil
	}
	limit := 0
	if c.rt != nil {
		limit = c.rt.maxPipelineOps
	}
	if limit > 0 && len(group) > limit {
		errs := make([]error, 0, (len(group)+limit-1)/limit)
		for i := 0; i < len(group); i += limit {
			end := i + limit
			if end > len(group) {
				end = len(group)
			}
			if err := ctx.Err(); err != nil {
				errs = append(errs, err)
				break
			}
			if err := c.metaPipelineDispatchWave(ctx, node, group[i:end], out); err != nil {
				errs = append(errs, err)
			}
		}
		return errors.Join(errs...)
	}
	return c.metaPipelineDispatchWave(ctx, node, group, out)
}

func (c *Client) metaPipelineDispatchWave(
	ctx context.Context, node *addr, group []preparedMetaOp, out []MetaResult,
) error {
	if len(group) == 0 {
		return nil
	}
	box := takeMetaBatchBuffer()
	index, err := buildMetaPipelineIndex(group)
	if err != nil {
		putMetaBatchBuffer(box)
		return err
	}
	var barrier MetaNoop
	need := 0
	for i := range group {
		p := &group[i]
		need += len(p.line) + len(p.body) + len(p.tail)
		if p.noop {
			barrier = p.barrier
		}
	}
	need += metaQuietBarrierLen(barrier)
	if cap(*box) < need {
		*box = make([]byte, 0, need)
	}
	raw := (*box)[:0]
	for i := range group {
		p := &group[i]
		raw = append(raw, p.line...)
		raw = append(raw, p.body...)
		raw = append(raw, p.tail...)
	}
	raw = appendMetaQuietBarrier(raw, barrier)
	*box = raw

	req := buildRequest(group[0].cmd, group[0].key, raw)
	req.rawBox = box
	req.rawPut = putMetaBatchBuffer
	resp := buildQuietBarrierResponse()
	resp.metaPipeC = c
	resp.metaPipeDst = out
	resp.metaPipeGroup = group
	resp.metaPipeIndex = index
	resetMetaPipeSeen(resp, len(group))
	defer releaseReqAndResp(req, resp)

	return c.finishMetaPipelineDispatch(ctx, node, group, out, req, resp)
}

func (c *Client) finishMetaPipelineDispatch(
	ctx context.Context,
	node *addr,
	group []preparedMetaOp,
	out []MetaResult,
	req *request,
	resp *response,
) error {
	dispatchErr := c.dispatchToAddr(withObserveSkip(ctx), node, req, resp)
	if dispatchErr != nil && !isChargeMetaFault(dispatchErr) {
		if resp.metaSawReply {
			invalidateMetaPipelineDest(c, out, group)
		}
		return dispatchErr
	}
	// Malformed/desync responses must not leave a mixed success wave; protocol
	// CLIENT_ERROR/SERVER_ERROR may keep already-matched opaque siblings.
	if errors.Is(dispatchErr, ErrMalformedResponse) {
		invalidateMetaPipelineDest(c, out, group)
	}
	for _, p := range group {
		if p.noop {
			storeMetaPipelineResult(c, out, p.idx, &p, MetaResult{Status: MetaStatusMN})
		}
	}
	for _, p := range group {
		if out[p.idx].Status != 0 {
			continue
		}
		if isChargeMetaFault(dispatchErr) {
			storeMetaPipelineResult(c, out, p.idx, &p, metaResultWithErr(nil, MetaStatusUnknown, dispatchErr))
			continue
		}
		switch {
		case p.noop:
			storeMetaPipelineResult(c, out, p.idx, &p, MetaResult{Status: MetaStatusMN})
		case bytes.Equal(p.cmd, _MgBytes):
			storeMetaPipelineResult(c, out, p.idx, &p, MetaResult{Status: MetaStatusEN})
		default:
			storeMetaPipelineResult(c, out, p.idx, &p, MetaResult{Status: MetaStatusHD})
		}
	}
	return dispatchErr
}

// invalidateMetaPipelineDest clears slots written by this node wave so a
// malformed response cannot leave opaque siblings looking successful.
func invalidateMetaPipelineDest(c *Client, out []MetaResult, group []preparedMetaOp) {
	for _, p := range group {
		if p.idx < 0 || p.idx >= len(out) {
			continue
		}
		storeMetaPipelineResult(c, out, p.idx, &p, MetaResult{})
	}
}

func metaQuietBarrierLen(f MetaNoop) int {
	n := len(_MnCRLFBytes)
	if f.ProxyHint != "" {
		n += 2 + len(f.ProxyHint)
	}
	if f.ProxyPath != "" {
		n += 2 + len(f.ProxyPath)
	}
	for _, tok := range f.Extra {
		n += 1 + len(tok) // leading space + token
	}
	return n
}

func finishMetaPipelineDest(c *Client, out []MetaResult, idx int, p *preparedMetaOp) {
	dst := &out[idx]
	dst.Status = dst.Item.status
	dst.Item.status = 0
	if p == nil || !p.returnKey {
		dst.Item.Key = nil
		if dst.Item.WireKey != nil {
			dst.Item.WireKey = dst.Item.WireKey[:0]
		}
		return
	}
	var fallback []byte
	if c != nil {
		fallback = c.stripNS(p.key)
	}
	copyMetaUserKey(&dst.Item, p.keepUser, c, fallback, true /* returnKey */)
}

func storeMetaPipelineResult(c *Client, out []MetaResult, idx int, p *preparedMetaOp, res MetaResult) {
	keepVal := out[idx].Item.Value
	keepWire := out[idx].Item.WireKey
	var keepUser, keepOpaque []byte
	if p != nil {
		keepUser = p.keepUser
		keepOpaque = p.keepOpaque
	}
	if p == nil || !p.returnKey {
		res.Item.Key = nil
		res.Item.WireKey = nil
	} else {
		var fallback []byte
		if c != nil {
			fallback = c.stripNS(p.key)
		}
		copyMetaUserKey(&res.Item, keepUser, c, fallback, true /* returnKey */)
	}
	res.Item.Value = ownValue(keepVal, res.Item.Value)
	if len(res.Item.Opaque) == 0 {
		if keepOpaque != nil {
			res.Item.Opaque = keepOpaque[:0]
		}
	} else if keepOpaque != nil {
		res.Item.Opaque = append(keepOpaque[:0], res.Item.Opaque...)
	}
	out[idx] = res
	if (p == nil || !p.returnKey) && keepWire != nil {
		out[idx].Item.WireKey = keepWire[:0]
	}
}
