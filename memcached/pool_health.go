package memcached

import (
	"context"
	"time"
)

// startIdleHealthPing launches a loop that versions cold idle pooled sockets.
// Close waits on healthWG after closing healthStop (Uber: goroutine lifecycle).
func (c *Client) startIdleHealthPing() {
	if c.rt == nil || c.rt.idleHealthPing <= 0 || c.rt.udp {
		return
	}
	if c.healthStop != nil {
		return
	}
	interval := c.rt.idleHealthPing
	c.healthStop = make(chan struct{}) // unbuffered stop signal
	c.healthWG.Add(1)
	go c.idleHealthLoop(interval)
}

func (c *Client) idleHealthLoop(interval time.Duration) {
	defer c.healthWG.Done()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			if c.closed.Load() {
				return
			}
			c.pingIdlePools(interval)
		case <-c.healthStop:
			return
		}
	}
}

func (c *Client) pingIdlePools(minIdle time.Duration) {
	type pair struct {
		pool *connPool
		node *addr
	}
	var pairs []pair
	func() {
		c.mu.RLock()
		defer c.mu.RUnlock()
		pairs = make([]pair, 0, len(c.connPools))
		for _, a := range c.addrs {
			if a == nil {
				continue
			}
			p := c.connPools[a.id()]
			if p != nil {
				pairs = append(pairs, pair{pool: p, node: a})
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, pe := range pairs {
		if pe.pool.closed.Load() {
			continue
		}
		c.pingOneIdle(ctx, pe.pool, pe.node, minIdle)
	}
}

func (c *Client) pingOneIdle(ctx context.Context, p *connPool, node *addr, minIdle time.Duration) {
	cn := p.takeColdIdle(minIdle)
	if cn == nil {
		return
	}

	req, resp := buildVersionCommand()
	defer releaseReqAndResp(req, resp)
	h := &connHolder{cn: cn}
	if err := c.sendOnConn(ctx, node, h, req, resp); err != nil || h.cn == nil {
		discard := cn
		if h.cn != nil {
			discard = h.cn
		}
		_ = p.discard(discard)
		return
	}
	_ = p.put(h.cn)
}

// takeColdIdle pops the coldest idle conn when unused for at least minIdle.
func (p *connPool) takeColdIdle(minIdle time.Duration) wireConn {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.idle) == 0 || p.closed.Load() {
		return nil
	}
	cn := p.idle[0]
	if cn == nil {
		return nil
	}
	if minIdle > 0 {
		if _, tooIdle := cn.idle(resolveClock(p.now)().Add(-minIdle)); !tooIdle {
			return nil
		}
	}
	copy(p.idle, p.idle[1:])
	p.idle[len(p.idle)-1] = nil
	p.idle = p.idle[:len(p.idle)-1]
	return cn
}
