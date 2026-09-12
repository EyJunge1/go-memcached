package memcached

import (
	"crypto/tls"
	"fmt"
)

// SetServers swaps cluster membership. Config.AllowSetServers must be set.
func (c *Client) SetServers(servers []Server) error {
	if c.rt == nil || !c.rt.allowSetServers {
		return ErrSetServersNotAllowed
	}
	return c.applyServers(servers, true /* updateSpec */)
}

func (c *Client) validateApplyServers(servers []Server) ([]*addr, error) {
	var tlsCfg *tls.Config
	if c.rt != nil && c.rt.tlsSet {
		tlsCfg = c.rt.tlsConfig
	}
	if err := validateTransportServers(servers, tlsCfg); err != nil {
		return nil, err
	}
	if len(servers) > 0 && c.rt != nil && serversAreUDP(servers) != c.rt.udp {
		return nil, fmt.Errorf("cannot change transport family: %w", ErrMixedTransport)
	}
	if c.rt != nil && (c.rt.authUser != "" || c.rt.authPass != "") {
		if err := validateAuthAgainstServers(servers, c.rt.authAllowPlain, c.rt.tlsSet); err != nil {
			return nil, err
		}
	}
	if len(servers) == 0 {
		return nil, nil
	}
	return serversToAddrs(servers)
}

func (c *Client) rotateConnPools(addrs []*addr, keep map[string]struct{}) map[string]*connPool {
	next := make(map[string]*connPool, len(addrs))
	for key, pool := range c.connPools {
		if _, ok := keep[key]; ok {
			next[key] = pool
			continue
		}
		_ = pool.closePool()
	}
	for _, a := range c.addrs {
		if a == nil {
			continue
		}
		if _, ok := keep[a.id()]; !ok {
			a.pool.Store(nil)
		}
	}
	return next
}

func (c *Client) pruneHealthForRemoved(keep map[string]struct{}) {
	c.healthMu.Lock()
	defer c.healthMu.Unlock()
	healthChanged := false
	for id := range c.failCount {
		if _, ok := keep[id]; !ok {
			delete(c.failCount, id)
			healthChanged = true
		}
	}
	for id := range c.deadUntil {
		if _, ok := keep[id]; !ok {
			delete(c.deadUntil, id)
			healthChanged = true
		}
	}
	if healthChanged {
		c.healthVersion.Add(1)
	}
	c.healthDirty.Store(len(c.failCount) != 0)
	c.deadDirty.Store(len(c.deadUntil) != 0)
}

func (c *Client) commitServerMembership(
	servers []Server, addrs []*addr, picker picker, next map[string]*connPool, updateSpec bool,
) {
	c.addrs = addrs
	c.picker = picker
	c.connPools = next
	c.clusterVersion.Add(1)
	c.publishSingleLocked()
	for _, a := range addrs {
		if a == nil {
			continue
		}
		if p, ok := next[a.id()]; ok {
			a.pool.Store(p)
		}
	}
	if updateSpec && !c.resolveSpecFixed {
		c.resolveSpec = formatServerSpec(servers)
	}
}

// applyServers swaps cluster membership and optionally updates the resolve spec.
func (c *Client) applyServers(servers []Server, updateSpec bool) error {
	// Copy so caller mutations during validate/format cannot race (Uber: copy at boundaries).
	if n := len(servers); n > 0 {
		cp := make([]Server, n)
		copy(cp, servers)
		servers = cp
	}
	addrs, err := c.validateApplyServers(servers)
	if err != nil {
		return err
	}
	picker := c.rt.pickBuilder.Build(addrs)
	keep := applyServersKeepSet(addrs)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return ErrClosed
	}
	if sameAddrMembership(c.addrs, addrs) {
		if updateSpec && !c.resolveSpecFixed {
			c.resolveSpec = formatServerSpec(servers)
		}
		return nil
	}

	next := c.rotateConnPools(addrs, keep)
	c.pruneHealthForRemoved(keep)
	c.commitServerMembership(servers, addrs, picker, next, updateSpec)
	return nil
}

func applyServersKeepSet(addrs []*addr) map[string]struct{} {
	keep := make(map[string]struct{}, len(addrs))
	for _, a := range addrs {
		keep[a.id()] = struct{}{}
	}
	return keep
}
