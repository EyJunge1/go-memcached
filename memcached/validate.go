package memcached

import (
	"encoding/base64"
	"fmt"
)

// validateKeyAndValue checks a text-protocol key and value against the default size limits.
func validateKeyAndValue(key, value []byte) error {
	return validateKeyAndValueFlagsLimit(key, value, false, defaultMaxValueBytes)
}

// validateKeyAndValueFlags checks a key and value, optionally treating the key as binary (Base64).
func validateKeyAndValueFlags(key, value []byte, binaryKey bool) error {
	return validateKeyAndValueFlagsLimit(key, value, binaryKey, defaultMaxValueBytes)
}

// checkKeyValue validates a text-protocol key and value against this client's value limit.
func (c *Client) checkKeyValue(key, value []byte) error {
	return validateKeyAndValueFlagsLimit(key, value, false, c.valueLimit())
}

// checkKeyValueFlags validates a key and value against this client's value limit.
func (c *Client) checkKeyValueFlags(key, value []byte, binaryKey bool) error {
	return validateKeyAndValueFlagsLimit(key, value, binaryKey, c.valueLimit())
}

// validateKeyAndValueFlagsLimit checks key length, ASCII whitespace/control
// bytes, and value size. Other high-bit bytes remain opaque and valid.
// binaryKey skips byte checks and compares Base64(key) to maxKeySize.
func validateKeyAndValueFlagsLimit(key, value []byte, binaryKey bool, limit int) error {
	nKey := len(key)
	if nKey == 0 {
		return ErrInvalidKey
	}
	if binaryKey {
		if base64.StdEncoding.EncodedLen(nKey) > maxKeySize {
			return ErrInvalidKey
		}
	} else if nKey > maxKeySize {
		return ErrInvalidKey
	}
	if !binaryKey {
		for _, b := range key {
			if b <= ' ' || b == 0x7f {
				return ErrInvalidKey
			}
		}
	}

	if limit <= 0 {
		limit = defaultMaxValueBytes
	}
	if nValue := len(value); nValue > limit {
		return ErrInvalidValue
	}

	return nil
}

// validNamespace rejects Cluster.Namespace values that would break text framing
// or leave no room for a non-empty key in the 250-byte wire-key budget.
func validNamespace(ns string) error {
	if ns == "" {
		return nil
	}
	if len(ns) >= maxKeySize {
		return fmt.Errorf("namespace too long: %w", ErrInvalidArgument)
	}
	for i := 0; i < len(ns); i++ {
		if ns[i] <= ' ' || ns[i] == 0x7f {
			return fmt.Errorf("namespace invalid: %w", ErrInvalidArgument)
		}
	}
	return nil
}
