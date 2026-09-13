package memcached

import "encoding/base64"

func validateKeyAndValue(key, value []byte) error {
	return validateKeyAndValueFlags(key, value, false)
}

func validateKeyAndValueFlags(key, value []byte, binaryKey bool) error {
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

	if nValue := len(value); nValue > maxValueSize {
		return ErrInvalidValue
	}

	return nil
}
