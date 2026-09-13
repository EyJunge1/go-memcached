package memcached

import (
	"context"
	"fmt"
)

// Authenticate performs memcached token auth (-Y): a fake set whose body is
// "username password". Prefer TLS or a Unix socket; plaintext exposes the token.
func (c *Client) Authenticate(ctx context.Context, username, password string) error {
	if err := validAuthSecret(username); err != nil {
		return err
	}
	if err := validAuthSecret(password); err != nil {
		return err
	}
	req, resp := buildAuthCommand(username, password)
	defer releaseReqAndResp(req, resp)
	resp.afterRecv = mapStorageReply
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return fmt.Errorf("auth failed: %w", err)
	}
	return nil
}

func validAuthSecret(s string) error {
	for i := 0; i < len(s); i++ {
		if s[i] == '\r' || s[i] == '\n' || s[i] == 0 {
			return fmt.Errorf("auth credential invalid: %w", ErrInvalidArgument)
		}
	}
	return nil
}

func buildAuthCommand(username, password string) (*request, *response) {
	payload := []byte(username + " " + password)
	return buildStorageCommand("set", "_", payload, 0, 0, false)
}
