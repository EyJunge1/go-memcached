package memcached

import "encoding/base64"

func validateKeyAndValue(key, value []byte) error {
	return validateKeyAndValueFlagsLimit(key, value, false, defaultMaxValueBytes)
}

func validateKeyAndValueFlags(key, value []byte, binaryKey bool) error {
	return validateKeyAndValueFlagsLimit(key, value, binaryKey, defaultMaxValueBytes)
}

func (c *Client) checkKeyValue(key, value []byte) error {
	return validateKeyAndValueFlagsLimit(key, value, false, c.valueLimit())
}

func (c *Client) checkKeyValueFlags(key, value []byte, binaryKey bool) error {
	return validateKeyAndValueFlagsLimit(key, value, binaryKey, c.valueLimit())
}

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
