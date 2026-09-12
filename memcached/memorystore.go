package memcached

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var _ resolver = (*memorystoreResolver)(nil)

// memorystoreResolver polls a Memorystore/GCP config endpoint with
// `config get cluster` and returns the node list when ConfigID advances.
// configID/last commit only after a successful Client.applyServers (see commitPending).
type memorystoreResolver struct {
	endpoint string
	rt       *runtime
	configID atomic.Int64
	last     atomic.Pointer[[]*addr]

	// pendingMu guards pendingID/pending/hasPending (named, not embedded).
	pendingMu  sync.Mutex
	pendingID  int64
	pending    []*addr
	hasPending bool

	// connMu guards the reused config endpoint connection.
	connMu   sync.Mutex
	conn     net.Conn
	connNet  string
	connAddr string
	br       *bufio.Reader
}

func newMemorystoreResolver(endpoint string, rt *runtime) *memorystoreResolver {
	return &memorystoreResolver{endpoint: strings.TrimSpace(endpoint), rt: rt}
}

// Resolve dials (or reuses) the config endpoint and parses `config get cluster`.
// Spec is ignored; the endpoint is fixed at construction (ResolveSpec / first Server).
// A new ConfigID is held as pending until commitPending after applyServers succeeds.
func (r *memorystoreResolver) Resolve(spec string) ([]*addr, error) {
	_ = spec
	if r == nil || r.endpoint == "" {
		return nil, fmt.Errorf("empty memorystore endpoint: %w", ErrInvalidAddress)
	}
	endpoint, err := cleanAddr(r.endpoint)
	if err != nil {
		return nil, err
	}
	var resolver defaultResolver
	network, address, err := resolver.resolveAddr(endpoint)
	if err != nil {
		return nil, err
	}
	if isUDPNetwork(network) {
		return nil, fmt.Errorf("memorystore discovery on UDP: %w", ErrNotSupported)
	}

	cfg, err := r.fetchConfig(network, address)
	if err != nil {
		return nil, err
	}
	if len(cfg.Nodes) == 0 {
		return nil, fmt.Errorf("memorystore config empty: %w", ErrInvalidAddress)
	}

	prev := r.configID.Load()
	if prev != 0 && cfg.ConfigID <= prev {
		if snap := r.last.Load(); snap != nil && len(*snap) > 0 {
			r.clearPending()
			return append([]*addr(nil), (*snap)...), nil
		}
	}

	addrs := make([]*addr, 0, len(cfg.Nodes))
	for _, n := range cfg.Nodes {
		addrs = append(addrs, newAddr("tcp", net.JoinHostPort(n.Host, strconv.FormatInt(n.Port, 10))))
	}
	func() {
		r.pendingMu.Lock()
		defer r.pendingMu.Unlock()
		r.pendingID = cfg.ConfigID
		r.pending = addrs
		r.hasPending = true
	}()
	return append([]*addr(nil), addrs...), nil
}

func (r *memorystoreResolver) fetchConfig(network, address string) (memorystoreConfig, error) {
	budget := memorystoreDialBudget(r.rt)
	r.connMu.Lock()
	defer r.connMu.Unlock()

	try := func(raw net.Conn, br *bufio.Reader) (memorystoreConfig, error) {
		_ = raw.SetDeadline(time.Now().Add(budget))
		if _, err := io.WriteString(raw, "config get cluster\r\n"); err != nil {
			return memorystoreConfig{}, fmt.Errorf("%w: memorystore write: %w", ErrTransport, err)
		}
		return parseMemorystoreConfig(br)
	}

	if r.conn != nil && r.connNet == network && r.connAddr == address {
		cfg, err := try(r.conn, r.br)
		if err == nil {
			return cfg, nil
		}
		_ = r.conn.Close()
		r.conn, r.br = nil, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	dial := r.rt.dialContext
	if dial == nil {
		dial = defaultRawDial(r.rt.keepAlive)
	}
	raw, err := dial(ctx, network, address)
	if err != nil {
		return memorystoreConfig{}, fmt.Errorf("%w: memorystore dial: %w", ErrTransport, err)
	}
	br := bufio.NewReader(raw)
	cfg, err := try(raw, br)
	if err != nil {
		_ = raw.Close()
		return memorystoreConfig{}, err
	}
	r.conn = raw
	r.br = br
	r.connNet = network
	r.connAddr = address
	return cfg, nil
}

func (r *memorystoreResolver) closeConn() {
	r.connMu.Lock()
	defer r.connMu.Unlock()
	if r.conn != nil {
		_ = r.conn.Close()
		r.conn, r.br = nil, nil
	}
}

func (r *memorystoreResolver) commitPending() {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()
	if !r.hasPending {
		return
	}
	// Defensive copy at the boundary so later pending reuse cannot mutate last.
	addrs := append([]*addr(nil), r.pending...)
	r.configID.Store(r.pendingID)
	r.last.Store(&addrs)
	r.hasPending = false
	r.pending = nil
}

func (r *memorystoreResolver) clearPending() {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()
	r.hasPending = false
	r.pending = nil
}

func (r *memorystoreResolver) pendingConfigID() int64 {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()
	if r.hasPending {
		return r.pendingID
	}
	return r.configID.Load()
}

func memorystoreDialBudget(rt *runtime) time.Duration {
	d := 3 * time.Second
	if rt != nil {
		if rt.dialTimeout > 0 {
			d = rt.dialTimeout
		}
		if rt.readTimeout > 0 && rt.readTimeout > d {
			d = rt.readTimeout
		}
	}
	return d
}

// memorystoreConfig is one `config get cluster` payload.
type memorystoreConfig struct {
	ConfigID int64
	Nodes    []memorystoreNode
}

type memorystoreNode struct {
	Host string
	Port int64
}

// parseMemorystoreConfig reads a CONFIG cluster response from r.
//
// Wire shape (Memorystore):
//
//	CONFIG cluster 0 <nbytes>\r\n
//	<config-id>\r\n
//	host|ip|port host|ip|port ...\r\n
//	\r\n
//	END\r\n
func parseMemorystoreConfig(r *bufio.Reader) (memorystoreConfig, error) {
	var out memorystoreConfig
	for {
		line, err := readMemorystoreLine(r)
		if err != nil {
			return out, err
		}
		if line == "" {
			continue
		}
		if strings.EqualFold(line, "END") {
			if len(out.Nodes) == 0 {
				return out, fmt.Errorf("memorystore config missing nodes: %w", ErrMalformedResponse)
			}
			return out, nil
		}
		if strings.HasPrefix(strings.ToUpper(line), "CONFIG") {
			idLine, err := readMemorystoreLine(r)
			if err != nil {
				return out, err
			}
			id, err := strconv.ParseInt(strings.TrimSpace(idLine), 10, 64)
			if err != nil {
				return out, fmt.Errorf("memorystore config id: %w", ErrMalformedResponse)
			}
			out.ConfigID = id

			nodesLine, err := readMemorystoreLine(r)
			if err != nil {
				return out, err
			}
			nodes, err := parseMemorystoreNodes(nodesLine)
			if err != nil {
				return out, err
			}
			out.Nodes = nodes
			continue
		}
		if strings.HasPrefix(line, "ERROR") ||
			strings.HasPrefix(line, "CLIENT_ERROR") ||
			strings.HasPrefix(line, "SERVER_ERROR") {
			return out, fmt.Errorf("memorystore config: %s: %w", line, ErrServerError)
		}
		return out, fmt.Errorf("memorystore unexpected line %q: %w", line, ErrMalformedResponse)
	}
}

func parseMemorystoreNodes(line string) ([]memorystoreNode, error) {
	line = strings.TrimSpace(line)
	if line == "" {
		return nil, fmt.Errorf("memorystore nodes empty: %w", ErrMalformedResponse)
	}
	parts := strings.Fields(line)
	out := make([]memorystoreNode, 0, len(parts))
	for _, part := range parts {
		fields := strings.Split(part, "|")
		if len(fields) < 3 {
			return nil, fmt.Errorf("memorystore node %q: %w", part, ErrMalformedResponse)
		}
		host := strings.TrimSpace(fields[1])
		if host == "" {
			host = strings.TrimSpace(fields[0])
		}
		port, err := strconv.ParseInt(strings.TrimSpace(fields[2]), 10, 64)
		if err != nil || port <= 0 || port > 65535 {
			return nil, fmt.Errorf("memorystore port in %q: %w", part, ErrMalformedResponse)
		}
		if host == "" {
			return nil, fmt.Errorf("memorystore host empty in %q: %w", part, ErrMalformedResponse)
		}
		out = append(out, memorystoreNode{Host: host, Port: port})
	}
	return out, nil
}

func readMemorystoreLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		if errors.Is(err, io.EOF) && len(line) == 0 {
			return "", fmt.Errorf("%w: memorystore EOF: %w", ErrTransport, err)
		}
		if !errors.Is(err, io.EOF) {
			return "", fmt.Errorf("%w: memorystore read: %w", ErrTransport, err)
		}
	}
	line = bytes.TrimSuffix(line, []byte{'\n'})
	line = bytes.TrimSuffix(line, []byte{'\r'})
	return string(line), nil
}
