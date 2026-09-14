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
	if !c.rt.failover.Eject || c.rt.failover.FailureLimit <= 0 || !c.deadDirty.Load() {
		return addrs
	}
	now := clock()
	c.healthMu.Lock()
	dead, candidates := 0, 0
	changed := false
	for _, a := range addrs {
		if a == nil {
			continue
		}
		candidates++
		id := a.id()
		until, ok := c.deadUntil[id]
		if !ok {
			continue
		}
		if now.Before(until) {
			dead++
		} else {
			delete(c.deadUntil, id)
			changed = true
		}
	}
	if changed {
		c.healthVersion.Add(1)
	}
	c.deadDirty.Store(len(c.deadUntil) != 0)
	if dead == 0 || dead == candidates {
		c.healthMu.Unlock()
		return addrs
	}
	live := make([]*addr, 0, len(addrs)-dead)
	for _, a := range addrs {
		if a == nil {
			continue
		}
		if until, ok := c.deadUntil[a.id()]; !ok || !now.Before(until) {
			live = append(live, a)
		}
	}
	c.healthMu.Unlock()
	return live
}

// isDead reports whether node is still inside its failover quarantine window.
func (c *Client) isDead(node *addr) bool {
	if node == nil || c.rt.failover.FailureLimit <= 0 || !c.deadDirty.Load() {
		return false
	}
	c.healthMu.Lock()
	defer c.healthMu.Unlock()
	id := node.id()
	until, ok := c.deadUntil[id]
	if !ok {
		return false
	}
	if clock().Before(until) {
		return true
	}
	delete(c.deadUntil, id)
	c.healthVersion.Add(1)
	c.deadDirty.Store(len(c.deadUntil) != 0)
	return false
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
	c.healthDirty.Store(true)
	if c.failCount[id] >= c.rt.failover.FailureLimit {
		before, existed := c.deadUntil[id]
		if dur := quarantineDuration(c.rt.failover, c.failCount[id]); dur > 0 {
			c.deadUntil[id] = clock().Add(dur)
		} else {
			c.deadUntil[id] = deadUntilRevived
		}
		if !existed || c.deadUntil[id] != before {
			c.healthVersion.Add(1)
		}
		c.deadDirty.Store(true)
	}
}

// noteSuccess clears the node's failure count and lifts any quarantine.
func (c *Client) noteSuccess(node *addr) {
	if node == nil || c.rt == nil || c.rt.failover.FailureLimit <= 0 || !c.healthDirty.Load() {
		return
	}
	c.healthMu.Lock()
	defer c.healthMu.Unlock()
	id := node.id()
	_, wasDead := c.deadUntil[id]
	delete(c.failCount, id)
	delete(c.deadUntil, id)
	if wasDead {
		c.healthVersion.Add(1)
	}
	c.healthDirty.Store(len(c.failCount) != 0)
	c.deadDirty.Store(len(c.deadUntil) != 0)
}

// DeadServers lists members that are in failover quarantine right now.
func (c *Client) DeadServers() []Server {
	addrs, _ := c.snapshotCluster()
	var out []Server
	if !c.deadDirty.Load() {
		return out
	}
	now := clock()
	c.healthMu.Lock()
	changed := false
	for _, a := range addrs {
		if a == nil {
			continue
		}
		id := a.id()
		until, ok := c.deadUntil[id]
		if !ok {
			continue
		}
		if now.Before(until) {
			out = append(out, addrToServer(a))
		} else {
			delete(c.deadUntil, id)
			changed = true
		}
	}
	if changed {
		c.healthVersion.Add(1)
	}
	c.deadDirty.Store(len(c.deadUntil) != 0)
	c.healthMu.Unlock()
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
				if c.closed.Load() {
					return
				}
				if err != nil {
					c.setResolverErr(err)
					continue
				}
				if c.sameResolvedAddrs(addrs) {
					c.setResolverErr(nil)
					continue
				}
				if err = c.applyServers(addrsToServers(addrs), false); err != nil {
					if errors.Is(err, ErrClosed) {
						return
					}
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

// sameResolvedAddrs reports whether a resolver refresh returned the current
// ordered membership, avoiding picker rebuilds and address churn on no-op ticks.
func (c *Client) sameResolvedAddrs(addrs []*addr) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return sameAddrMembership(c.addrs, addrs)
}

func sameAddrMembership(current, next []*addr) bool {
	if len(current) != len(next) {
		return false
	}
	for i, a := range next {
		have := current[i]
		if a == nil || have == nil {
			if a != have {
				return false
			}
			continue
		}
		if a.Network != have.Network || a.Address != have.Address || a.Weight != have.Weight {
			return false
		}
	}
	return true
}
