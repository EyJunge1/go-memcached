package memcached

import (
	"fmt"
)

const maxOpaqueTokenBytes = 32

// validMetaHints rejects whitespace or controls in proxy tokens P and L.
func validMetaHints(p, l string) error {
	if err := validMetaHint(p); err != nil {
		return fmt.Errorf("hint p: %w", err)
	}
	if err := validMetaHint(l); err != nil {
		return fmt.Errorf("hint l: %w", err)
	}
	return nil
}

// validMetaHint rejects whitespace or controls in one proxy hint token.
func validMetaHint(tok string) error {
	if tok == "" {
		return nil
	}
	for i := 0; i < len(tok); i++ {
		if tok[i] <= ' ' || tok[i] == 0x7f {
			return fmt.Errorf("hint token invalid: %w", ErrInvalidArgument)
		}
	}
	return nil
}

// validOpaqueToken rejects empty, overlong, or control-bearing opaque tokens.
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
