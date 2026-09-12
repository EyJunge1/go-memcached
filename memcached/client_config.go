package memcached

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
)

// ConfigGetResult is one `config get <type>` payload (Memorystore-style).
type ConfigGetResult struct {
	Type string
	Body []byte
}

// GetConfig runs `config get <type>` on the first live node.
// Empty typ defaults to "cluster".
func (c *Client) GetConfig(ctx context.Context, typ string, dst *ConfigGetResult) error {
	if dst == nil {
		return fmt.Errorf("config get dst: %w", ErrInvalidArgument)
	}
	if typ == "" {
		typ = "cluster"
	}
	if strings.ContainsAny(typ, " \r\n") {
		return fmt.Errorf("config type: %w", ErrInvalidArgument)
	}
	node, err := c.pickFirstLiveAddr()
	if err != nil {
		return err
	}
	cn, err := c.getConn(ctx, node)
	if err != nil {
		return wrapTransport(fmt.Errorf("alloc connection: %w", err))
	}
	var opErr error
	defer func() { finishDispatch(cn, opErr) }()

	cmd := "config get " + typ + "\r\n"
	stop := prepareOpIO(ctx, cn, c.rt.readTimeout, c.rt.writeTimeout)
	defer stop()
	if _, err = io.WriteString(cn, cmd); err != nil {
		opErr = wrapTransport(err)
		return opErr
	}
	if err = cn.Flush(); err != nil {
		opErr = wrapTransport(err)
		return opErr
	}
	body, err := readConfigGetBody(cn)
	if err != nil {
		opErr = err
		return err
	}
	dst.Type = typ
	dst.Body = append(dst.Body[:0], body...)
	return nil
}

func readConfigGetBody(cn wireConn) ([]byte, error) {
	var out []byte
	for {
		line, err := cn.readLine('\n')
		if err != nil {
			return nil, fmt.Errorf("%w: config get read: %w", ErrTransport, err)
		}
		trimmed := bytes.TrimSpace(line)
		if bytes.EqualFold(trimmed, _EndBytes) {
			return out, nil
		}
		upper := bytes.ToUpper(trimmed)
		if bytes.HasPrefix(upper, _ErrorBytes) ||
			bytes.HasPrefix(upper, _ClientErrorBytes) ||
			bytes.HasPrefix(upper, _ServerErrorBytes) {
			return nil, fmt.Errorf("config get: %s: %w", trimmed, ErrServerError)
		}
		out = append(out, line...)
		if !bytes.HasSuffix(line, []byte{'\n'}) {
			out = append(out, '\n')
		}
	}
}
