package memcached

import (
	"fmt"
)

const maxOpaqueTokenBytes = 32

func validOpaqueToken(token string) error {
	if token == "" {
		return fmt.Errorf("opaque token empty: %w", ErrInvalidArgument)
	}
	if len(token) > maxOpaqueTokenBytes {
		return fmt.Errorf("opaque token too long: %w", ErrInvalidArgument)
	}
	for i := 0; i < len(token); i++ {
		if token[i] <= ' ' || token[i] == 0x7f {
			return fmt.Errorf("opaque token invalid: %w", ErrInvalidArgument)
		}
	}
	return nil
}
