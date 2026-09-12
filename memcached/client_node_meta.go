package memcached

import (
	"context"
	"errors"
	"fmt"
)

// Get is Meta Get on the pinned node.
func (m *NodeMetaClient) Get(ctx context.Context, key []byte, dst *MetaResult, f MetaGet) error {
	if m == nil || m.n == nil {
		return ErrClosed
	}
	c, node, err := m.n.resolve()
	if err != nil {
		return err
	}
	return c.metaGet(ctx, node, key, dst, f)
}

// GetOne is an alias for Get (parity with Client.GetOne).
func (m *NodeMetaClient) GetOne(ctx context.Context, key []byte, dst *MetaResult, f MetaGet) error {
	return m.Get(ctx, key, dst, f)
}

// Debug is Meta Debug on the pinned node.
func (m *NodeMetaClient) Debug(ctx context.Context, key []byte, dst *MetaItemDebug, f MetaDebug) error {
	if m == nil || m.n == nil {
		return ErrClosed
	}
	c, node, err := m.n.resolve()
	if err != nil {
		return err
	}
	return c.metaDebug(ctx, node, key, dst, f)
}

// Noop is Meta Noop on the pinned node.
func (m *NodeMetaClient) Noop(ctx context.Context, dst *MetaResult, f MetaNoop) error {
	if m == nil || m.n == nil {
		return ErrClosed
	}
	c, node, err := m.n.resolve()
	if err != nil {
		return err
	}
	return c.metaNoop(ctx, node, dst, f)
}

// GetOrFill is Meta GetOrFill on the pinned node.
func (m *NodeMetaClient) GetOrFill(
	ctx context.Context, key []byte, dst *MetaResult, opts GetOrFillOpts, fill FillFunc,
) error {
	return metaGetOrFill(ctx, m, key, dst, opts, fill)
}

// PreparePipeline prepares mixed meta operations pinned to this node (hash skipped).
// dst and its reusable buffers must remain valid until Close.
func (m *NodeMetaClient) PreparePipeline(ops []MetaOp, dst []MetaResult) (*PreparedMetaBatch, error) {
	if m == nil || m.n == nil {
		return nil, ErrClosed
	}
	c, _, err := m.n.resolve()
	if err != nil {
		return nil, err
	}
	pin := m.n.target
	return prepareMetaPipeline(c, ops, dst, &pin)
}

// Deferred starts a deferred meta batch pinned to this node.
// A nil or unbound NodeMetaClient yields a handle whose Flush returns ErrClosed.
func (m *NodeMetaClient) Deferred(ctx context.Context) *DeferredMeta {
	if m == nil || m.n == nil {
		return &DeferredMeta{ctx: ctx}
	}
	return &DeferredMeta{pipe: m, ctx: ctx}
}

// Set is Meta Set on the pinned node.
func (m *NodeMetaClient) Set(ctx context.Context, key, value []byte, dst *MetaResult, f MetaSet) error {
	if m == nil || m.n == nil {
		return ErrClosed
	}
	c, node, err := m.n.resolve()
	if err != nil {
		return err
	}
	req, resp, keep, userKey, quiet, returnKey, err := prepareMetaSet(c, key, value, dst, f)
	if err != nil {
		return err
	}
	return finishMetaDestOnAddr(ctx, c, node, req, resp, dst, keep, quiet, false /* quietMiss */, returnKey, userKey)
}

// Store maps item onto Meta Set on the pinned node (parity with Client.Store).
func (m *NodeMetaClient) Store(ctx context.Context, item Item, dst *MetaResult) error {
	f, err := itemToMetaSet(item)
	if err != nil {
		return err
	}
	return m.Set(ctx, item.Key, item.Value, dst, f)
}

// Delete is Meta Delete on the pinned node.
// Semantics match MetaClient.Delete (quietMiss false).
func (m *NodeMetaClient) Delete(ctx context.Context, key []byte, dst *MetaResult, f MetaDelete) error {
	if m == nil || m.n == nil {
		return ErrClosed
	}
	c, node, err := m.n.resolve()
	if err != nil {
		return err
	}
	req, resp, keep, userKey, quiet, returnKey, err := prepareMetaDelete(c, key, dst, f)
	if err != nil {
		return err
	}
	return finishMetaDestOnAddr(ctx, c, node, req, resp, dst, keep, quiet, false /* quietMiss */, returnKey, userKey)
}

// Arithmetic is Meta Arithmetic on the pinned node.
// Semantics match MetaClient.Arithmetic (quietMiss false).
func (m *NodeMetaClient) Arithmetic(
	ctx context.Context,
	key []byte,
	delta uint64,
	dst *MetaResult,
	f MetaArithmetic,
) error {
	if m == nil || m.n == nil {
		return ErrClosed
	}
	c, node, err := m.n.resolve()
	if err != nil {
		return err
	}
	req, resp, keep, userKey, quiet, returnKey, err := prepareMetaArithmetic(c, key, delta, dst, f)
	if err != nil {
		return err
	}
	return finishMetaDestOnAddr(ctx, c, node, req, resp, dst, keep, quiet, false /* quietMiss */, returnKey, userKey)
}

// GetMulti fetches keys on the pinned node via Meta quiet gets + mn.
// Keys are borrowed until GetMulti returns (same as Meta().Pipeline).
func (m *NodeMetaClient) GetMulti(ctx context.Context, keys [][]byte, dst []MetaResult, f MetaGet) error {
	if m == nil || m.n == nil {
		return ErrClosed
	}
	if len(keys) != len(dst) {
		return fmt.Errorf("meta get multi dst length %d != keys %d: %w", len(dst), len(keys), ErrInvalidArgument)
	}
	if len(keys) == 0 {
		return nil
	}
	return m.Pipeline(ctx, metaGetMultiOps(keys, f), dst)
}

// StoreMulti stores items on the pinned node via Meta quiet sets + mn.
// dst must have len(items). Keys and values are borrowed until StoreMulti returns.
func (m *NodeMetaClient) StoreMulti(ctx context.Context, items []Item, dst []MetaResult) error {
	if m == nil || m.n == nil {
		return ErrClosed
	}
	if len(items) != len(dst) {
		return fmt.Errorf("meta store multi dst length %d != items %d: %w", len(dst), len(items), ErrInvalidArgument)
	}
	if len(items) == 0 {
		return nil
	}
	ops, err := metaStoreMultiOps(items)
	if err != nil {
		return err
	}
	err = m.Pipeline(ctx, ops, dst)
	return joinMetaResults(err, dst, false /* ignoreNotFound */)
}

// DeleteMulti deletes keys on the pinned node via Meta quiet deletes + mn.
// dst must have len(keys). Keys are borrowed until DeleteMulti returns.
func (m *NodeMetaClient) DeleteMulti(
	ctx context.Context, keys [][]byte, misses DeleteMissMode, dst []MetaResult,
) error {
	if m == nil || m.n == nil {
		return ErrClosed
	}
	if err := validDeleteMissMode(misses); err != nil {
		return err
	}
	if len(keys) != len(dst) {
		return fmt.Errorf("meta delete multi dst length %d != keys %d: %w", len(dst), len(keys), ErrInvalidArgument)
	}
	if len(keys) == 0 {
		return nil
	}
	err := m.Pipeline(ctx, metaDeleteMultiOps(keys), dst)
	return joinMetaResults(err, dst, misses != DeleteMissStrict)
}

func finishMetaDestOnAddr(
	ctx context.Context, c *Client, node *addr, req *request, resp *response,
	dst *MetaResult, keep metaKeep, quiet, quietMiss, returnKey bool, userKey []byte,
) error {
	resp.metaItem = &dst.Item
	defer releaseReqAndResp(req, resp)
	if err := c.dispatchToAddr(ctx, node, req, resp); err != nil {
		resetMetaGetDest(dst, keep)
		return fmt.Errorf("request: %w", err)
	}
	if err := resp.metaStatusErr(quiet, quietMiss); err != nil {
		if cd := dst.Item.status.Err(); cd != nil && errors.Is(err, cd) {
			applyMetaMutateDest(dst, keep, c, userKey, returnKey)
			return nil
		}
		resetMetaGetDest(dst, keep)
		return fmt.Errorf("request: %w", err)
	}
	applyMetaMutateDest(dst, keep, c, userKey, returnKey)
	return nil
}
