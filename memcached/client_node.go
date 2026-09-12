package memcached

import (
	"context"
	"fmt"
)

// NodeClient sends data-plane ops to one configured server via that node's pool.
// Unlike Session, it does not lease a connection; keys are not hash-checked.
// Use Meta() for quiet multi/meta batches pinned to the same node.
type NodeClient struct {
	c      *Client
	target Server
}

// NodeMetaClient is Meta() scoped to one configured server (hash skipped).
type NodeMetaClient struct {
	n *NodeClient
}

// On returns a handle pinned to server. server must be in the current membership.
func (c *Client) On(server Server) *NodeClient {
	if c == nil {
		return nil
	}
	return &NodeClient{c: c, target: server}
}

// Meta returns a meta-protocol handle pinned to this node.
func (n *NodeClient) Meta() *NodeMetaClient {
	if n == nil {
		return nil
	}
	return &NodeMetaClient{n: n}
}

func (n *NodeClient) resolve() (*Client, *addr, error) {
	if n == nil || n.c == nil {
		return nil, nil, ErrClosed
	}
	if n.c.closed.Load() {
		return nil, nil, ErrClosed
	}
	addrs, _ := n.c.snapshotCluster()
	wantNet := serverNetwork(n.target)
	for _, a := range addrs {
		if a != nil && a.Address == n.target.Address && serverNetwork(addrToServer(a)) == wantNet {
			return n.c, a, nil
		}
	}
	return nil, nil, fmt.Errorf("node target %s: %w", n.target.Address, ErrInvalidAddress)
}

// Store writes item on the pinned node.
func (n *NodeClient) Store(ctx context.Context, item Item) error {
	c, node, err := n.resolve()
	if err != nil {
		return err
	}
	return c.storeOnAddr(ctx, node, item)
}

// GetOne reads one key from the pinned node.
func (n *NodeClient) GetOne(ctx context.Context, key []byte, dst *GetResult, opts GetOpts) error {
	c, node, err := n.resolve()
	if err != nil {
		return err
	}
	return c.getOneOnAddr(ctx, node, key, dst, opts)
}

// Delete removes key on the pinned node.
func (n *NodeClient) Delete(ctx context.Context, key []byte, opts MutateOpts) error {
	c, node, err := n.resolve()
	if err != nil {
		return err
	}
	return c.deleteKey(ctx, node, key, opts)
}

// Touch updates expiry on the pinned node.
func (n *NodeClient) Touch(ctx context.Context, key []byte, expiry Exptime, opts MutateOpts) error {
	c, node, err := n.resolve()
	if err != nil {
		return err
	}
	return c.touchKey(ctx, node, key, expiry, opts)
}

// Incr adds delta to the key on the pinned node and writes the updated counter into dst.
func (n *NodeClient) Incr(ctx context.Context, key []byte, delta uint64, dst *uint64, opts ArithmeticOpts) error {
	c, node, err := n.resolve()
	if err != nil {
		return err
	}
	return c.arithmetic(ctx, node, "incr", key, delta, dst, opts)
}

// Decr subtracts delta from the key on the pinned node and writes the updated counter into dst.
func (n *NodeClient) Decr(ctx context.Context, key []byte, delta uint64, dst *uint64, opts ArithmeticOpts) error {
	c, node, err := n.resolve()
	if err != nil {
		return err
	}
	return c.arithmetic(ctx, node, "decr", key, delta, dst, opts)
}

// Get loads keys from the pinned node into dst (hash skipped).
// len(dst) must equal len(keys). Semantics match Client.Get.
func (n *NodeClient) Get(ctx context.Context, keys [][]byte, dst []GetResult, opts GetOpts) error {
	c, node, err := n.resolve()
	if err != nil {
		return err
	}
	if len(keys) != len(dst) {
		return fmt.Errorf("get dst length %d != keys %d: %w", len(dst), len(keys), ErrInvalidArgument)
	}
	if len(keys) == 0 {
		return nil
	}
	resetGetResults(dst)
	command, withCAS := getCommand(opts)
	var exp Exptime
	if opts.Touch != nil {
		exp = *opts.Touch
	}
	wired, box, hdrp, err := c.wireGetKeys(keys)
	if err != nil {
		return err
	}
	defer releaseWiredGetKeys(box, hdrp)
	return c.getFromAddr(ctx, node, command, exp, wired, nil, dst, withCAS, opts.Extra)
}

// Pipeline runs quiet meta ops on the pinned node (hash skipped).
func (m *NodeMetaClient) Pipeline(ctx context.Context, ops []MetaOp, dst []MetaResult) error {
	if m == nil || m.n == nil {
		return ErrClosed
	}
	c, node, err := m.n.resolve()
	if err != nil {
		return err
	}
	if len(ops) != len(dst) {
		return fmt.Errorf("meta pipeline dst length %d != ops %d: %w", len(dst), len(ops), ErrInvalidArgument)
	}
	if len(ops) == 0 {
		return nil
	}
	for i := range dst {
		_ = takeMetaKeep(&dst[i])
	}
	prepBox := getPreparedMeta(len(ops))
	defer putPreparedMeta(prepBox)
	for i, op := range ops {
		p, err := encodeMetaPipelineOp(c, op, i)
		if err != nil {
			return err
		}
		p.keepUser = dst[i].Item.Key
		p.keepOpaque = dst[i].Item.Opaque
		*prepBox = append(*prepBox, p)
	}
	return c.metaPipelineDispatch(ctx, node, *prepBox, dst)
}
