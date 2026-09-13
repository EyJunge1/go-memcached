package memcached

import (
	"errors"
	"time"
)

func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	if isKeepaliveProtocolError(err) ||
		errors.Is(err, ErrMalformedResponse) ||
		errors.Is(err, ErrServerError) ||
		errors.Is(err, ErrCommandError) {
		return false
	}
	return isTransportError(err)
}

func (c *Client) liveAddrs(addrs []*addr) []*addr {
	if !c.options.failover.Eject || c.options.failover.FailureLimit <= 0 {
		return addrs
	}
	live := make([]*addr, 0, len(addrs))
	for _, a := range addrs {
		if !c.isDead(a) {
			live = append(live, a)
		}
	}
	if len(live) == 0 {
		return addrs
	}
	return live
}

func (c *Client) isDead(node *addr) bool {
	if node == nil || c.options.failover.FailureLimit <= 0 {
		return false
	}
	c.healthMu.Lock()
	defer c.healthMu.Unlock()
	until, ok := c.deadUntil[node.id()]
	return ok && nowFunc().Before(until)
}

// deadUntilRevived is used when RetryAfter and DeadAfter are both 0:
// the node stays ejected until noteSuccess.
var deadUntilRevived = time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)

func quarantineDuration(fo Failover, failures int) time.Duration {
	if failures > fo.FailureLimit && fo.DeadAfter > 0 {
		return fo.DeadAfter
	}
	if fo.RetryAfter > 0 {
		return fo.RetryAfter
	}
	return fo.DeadAfter
}

func (c *Client) noteFailure(node *addr) {
	if node == nil || c.options.failover.FailureLimit <= 0 {
		return
	}
	c.healthMu.Lock()
	defer c.healthMu.Unlock()
	id := node.id()
	c.failCount[id]++
	if c.failCount[id] >= c.options.failover.FailureLimit {
		if dur := quarantineDuration(c.options.failover, c.failCount[id]); dur > 0 {
			c.deadUntil[id] = nowFunc().Add(dur)
		} else {
			c.deadUntil[id] = deadUntilRevived
		}
	}
}

func (c *Client) noteSuccess(node *addr) {
	if node == nil {
		return
	}
	c.healthMu.Lock()
	defer c.healthMu.Unlock()
	id := node.id()
	delete(c.failCount, id)
	delete(c.deadUntil, id)
}

// DeadServers returns nodes currently in failover quarantine.
func (c *Client) DeadServers() []Server {
	addrs, _ := c.snapshotCluster()
	var out []Server
	for _, a := range addrs {
		if c.isDead(a) {
			out = append(out, addrToServer(a))
		}
	}
	return out
}

func (c *Client) startResolverRefresh() {
	if c.options == nil || c.options.resolverRefresh <= 0 {
		return
	}
	c.refreshStop = make(chan struct{})
	go func() {
		t := time.NewTicker(c.options.resolverRefresh)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				c.mu.RLock()
				spec := c.resolveSpec
				c.mu.RUnlock()
				addrs, err := c.options.resolver.Resolve(spec)
				if err != nil {
					c.setResolverErr(err)
					continue
				}
				if err = c.applyServers(addrsToServers(addrs), false); err != nil {
					c.setResolverErr(err)
					continue
				}
				c.setResolverErr(nil)
			case <-c.refreshStop:
				return
			}
		}
	}()
}
