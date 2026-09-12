package memcached

import (
	"errors"
	"time"
)

// liveAddrs drops members currently in failover quarantine, unless every member is dead.
func (c *Client) liveAddrs(addrs []*addr) []*addr {
	if !c.rt.failover.Eject || c.rt.failover.FailureLimit <= 0 || !c.deadDirty.Load() {
		return addrs
	}
	now := resolveClock(c.now)()
	c.healthMu.Lock()
	defer c.healthMu.Unlock()
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
		if !now.Before(until) {
			delete(c.deadUntil, id)
			changed = true
			continue
		}
		dead++
	}
	if changed {
		c.healthVersion.Add(1)
	}
	c.deadDirty.Store(len(c.deadUntil) != 0)
	if dead == 0 || dead == candidates {
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
	if resolveClock(c.now)().Before(until) {
		return true
	}
	delete(c.deadUntil, id)
	c.healthVersion.Add(1)
	c.deadDirty.Store(len(c.deadUntil) != 0)
	return false
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
		until := _deadUntilRevived
		if dur := quarantineDuration(c.rt.failover, c.failCount[id]); dur > 0 {
			until = resolveClock(c.now)().Add(dur)
		}
		c.deadUntil[id] = until
		if !existed || until != before {
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
	if !c.deadDirty.Load() {
		return nil
	}
	now := resolveClock(c.now)()
	c.healthMu.Lock()
	defer c.healthMu.Unlock()
	out := make([]Server, 0, len(addrs))
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
		if !now.Before(until) {
			delete(c.deadUntil, id)
			changed = true
			continue
		}
		out = append(out, addrToServer(a))
	}
	if changed {
		c.healthVersion.Add(1)
	}
	c.deadDirty.Store(len(c.deadUntil) != 0)
	return out
}

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

// _deadUntilRevived is the quarantine end when RetryAfter and DeadAfter are
// both 0: the node stays out until noteSuccess.
var _deadUntilRevived = time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)

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
