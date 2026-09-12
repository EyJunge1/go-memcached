package memcached

import (
	"context"
	"errors"
	"time"
)

// startResolverRefresh launches the periodic Cluster.ResolverRefresh loop when
// that interval is set. Consecutive resolve/apply failures use capped
// exponential backoff (with jitter) instead of the fixed ticker interval; a
// successful tick resets the delay.
func (c *Client) startResolverRefresh() {
	if c.rt == nil || c.rt.resolverRefresh <= 0 {
		return
	}
	c.refreshStop = make(chan struct{})
	c.refreshWG.Add(1)
	go func() {
		defer c.refreshWG.Done()
		base := c.rt.resolverRefresh
		delay := base
		fails := 0
		timer := time.NewTimer(delay)
		defer timer.Stop()
		for {
			select {
			case <-timer.C:
				if c.refreshMembership(false /* fromTicker */) {
					return
				}
				fails = 0
				delay = base
				if c.ResolverErr() != nil {
					fails++
					delay = discoveryBackoff(base, fails)
				}
				timer.Reset(delay)
			case <-c.refreshStop:
				return
			}
		}
	}()
}

// refreshMembership runs one resolver tick. The bool marks the call site
// (New vs ticker); behavior is identical. When the client is already closed,
// it returns true so the refresh goroutine can exit. Memorystore ConfigID
// commits only after a successful applyServers.
func (c *Client) refreshMembership(_ bool) (stop bool) {
	if c.rt == nil || c.rt.resolver == nil {
		return false
	}
	var spec string
	func() {
		c.mu.RLock()
		defer c.mu.RUnlock()
		spec = c.resolveSpec
	}()
	start := time.Now()
	addrs, err := c.rt.resolver.Resolve(spec)
	if c.closed.Load() {
		c.clearResolverPending()
		return true
	}
	configID := resolverPendingConfigID(c.rt.resolver)
	if err != nil {
		c.clearResolverPending()
		c.setResolverErr(err)
		c.observeDiscovery(start, configID, 0, err)
		return false
	}
	if c.sameResolvedAddrs(addrs) {
		c.commitResolverPending()
		c.setResolverErr(nil)
		c.observeDiscovery(start, configID, len(addrs), nil)
		return false
	}
	if err = c.applyServers(addrsToServers(addrs), false /* updateSpec */); err != nil {
		c.clearResolverPending()
		if errors.Is(err, ErrClosed) {
			c.setResolverErr(err)
			c.observeDiscovery(start, configID, len(addrs), err)
			return true
		}
		c.setResolverErr(err)
		c.observeDiscovery(start, configID, len(addrs), err)
		return false
	}
	c.commitResolverPending()
	c.setResolverErr(nil)
	c.observeDiscovery(start, configID, len(addrs), nil)
	return false
}

func (c *Client) commitResolverPending() {
	if ms, ok := c.rt.resolver.(*memorystoreResolver); ok {
		ms.commitPending()
	}
}

func (c *Client) clearResolverPending() {
	if c.rt == nil {
		return
	}
	if ms, ok := c.rt.resolver.(*memorystoreResolver); ok {
		ms.clearPending()
	}
}

func (c *Client) observeDiscovery(start time.Time, configID int64, nodes int, err error) {
	c.observeEvent(context.Background(), OpEvent{
		Op:       OpDiscovery,
		Duration: time.Since(start),
		Err:      err,
		Keys:     nodes,
		ConfigID: configID,
	})
}

// sameResolvedAddrs reports whether a resolver refresh returned the current
// ordered membership, avoiding picker rebuilds and address churn on no-op ticks.
func (c *Client) sameResolvedAddrs(addrs []*addr) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return sameAddrMembership(c.addrs, addrs)
}

// discoveryBackoff returns min(base*2^(fails-1), 5m) with up to 20% jitter.
func discoveryBackoff(base time.Duration, fails int) time.Duration {
	if fails < 1 {
		fails = 1
	}
	d := base
	for i := 1; i < fails && d < 5*time.Minute; i++ {
		d *= 2
	}
	if d > 5*time.Minute {
		d = 5 * time.Minute
	}
	if d <= 0 {
		return base
	}
	// Jitter: subtract up to 20% so stacked clients do not stampede.
	jitter := time.Duration(int64(d) / 5 * (time.Now().UnixNano() % 100) / 100)
	if jitter >= d {
		return d
	}
	return d - jitter
}

func resolverPendingConfigID(r resolver) int64 {
	if ms, ok := r.(*memorystoreResolver); ok {
		return ms.pendingConfigID()
	}
	return 0
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
		if a.Network != have.Network || a.Address != have.Address {
			return false
		}
	}
	return true
}
