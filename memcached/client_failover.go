package memcached

import (
	"errors"
	"time"
)

// isRetryable reports whether err is a transport failure that dispatch may try on another attempt.
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

// liveAddrs drops members currently in failover quarantine, unless every member is dead.
func (c *Client) liveAddrs(addrs []*addr) []*addr {
	if !c.rt.failover.Eject || c.rt.failover.FailureLimit <= 0 {
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

// isDead reports whether node is still inside its failover quarantine window.
func (c *Client) isDead(node *addr) bool {
	if node == nil || c.rt.failover.FailureLimit <= 0 {
		return false
	}
	c.healthMu.Lock()
	defer c.healthMu.Unlock()
	until, ok := c.deadUntil[node.id()]
	return ok && clock().Before(until)
}

// deadUntilRevived is the quarantine end when RetryAfter and DeadAfter are
// both 0: the node stays out until noteSuccess.
var deadUntilRevived = time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)

// quarantineDuration returns RetryAfter, or DeadAfter once failures exceed FailureLimit.
func quarantineDuration(fo Failover, failures int) time.Duration {
	if failures > fo.FailureLimit && fo.DeadAfter > 0 {
		return fo.DeadAfter
	}
	if fo.RetryAfter > 0 {
		return fo.RetryAfter
	}
	return fo.DeadAfter
}

// noteFailure increments the node's failure count and quarantines it once FailureLimit is reached.
func (c *Client) noteFailure(node *addr) {
	if node == nil || c.rt.failover.FailureLimit <= 0 {
		return
	}
	c.healthMu.Lock()
	defer c.healthMu.Unlock()
	id := node.id()
	c.failCount[id]++
	if c.failCount[id] >= c.rt.failover.FailureLimit {
		if dur := quarantineDuration(c.rt.failover, c.failCount[id]); dur > 0 {
			c.deadUntil[id] = clock().Add(dur)
		} else {
			c.deadUntil[id] = deadUntilRevived
		}
	}
}

// noteSuccess clears the node's failure count and lifts any quarantine.
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

// DeadServers lists members that are in failover quarantine right now.
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

// startResolverRefresh launches the periodic Cluster.ResolverRefresh loop when that interval is set.
func (c *Client) startResolverRefresh() {
	if c.rt == nil || c.rt.resolverRefresh <= 0 {
		return
	}
	c.refreshStop = make(chan struct{})
	go func() {
		t := time.NewTicker(c.rt.resolverRefresh)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				c.mu.RLock()
				spec := c.resolveSpec
				c.mu.RUnlock()
				addrs, err := c.rt.resolver.Resolve(spec)
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
