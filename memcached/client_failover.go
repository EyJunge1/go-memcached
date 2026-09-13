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
		errors.Is(err, ErrUnknownIndicator) {
		return false
	}
	return isTransportError(err)
}

func (c *Client) liveAddrs(addrs []*Addr) []*Addr {
	if !c.options.failover.EjectFromHash || c.options.failover.FailureLimit <= 0 {
		return addrs
	}
	live := make([]*Addr, 0, len(addrs))
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

func (c *Client) isDead(addr *Addr) bool {
	if addr == nil || c.options.failover.FailureLimit <= 0 {
		return false
	}
	c.healthMu.Lock()
	defer c.healthMu.Unlock()
	until, ok := c.deadUntil[addr.id()]
	return ok && nowFunc().Before(until)
}

func (c *Client) noteFailure(addr *Addr) {
	if addr == nil || c.options.failover.FailureLimit <= 0 {
		return
	}
	c.healthMu.Lock()
	defer c.healthMu.Unlock()
	id := addr.id()
	c.failCount[id]++
	if c.failCount[id] >= c.options.failover.FailureLimit {
		c.deadUntil[id] = nowFunc().Add(c.options.failover.RetryTimeout)
	}
}

func (c *Client) noteSuccess(addr *Addr) {
	if addr == nil {
		return
	}
	c.healthMu.Lock()
	defer c.healthMu.Unlock()
	id := addr.id()
	delete(c.failCount, id)
	delete(c.deadUntil, id)
}

// DeadServers returns address IDs currently in failover quarantine.
func (c *Client) DeadServers() []string {
	c.healthMu.Lock()
	defer c.healthMu.Unlock()
	now := nowFunc()
	var out []string
	for id, until := range c.deadUntil {
		if now.Before(until) {
			out = append(out, id)
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
				raw := c.addrString
				c.mu.RUnlock()
				_ = c.SetServers(raw)
			case <-c.refreshStop:
				return
			}
		}
	}()
}
