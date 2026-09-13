package memcached

import (
	"bytes"
	"testing"
)

func Test_validOpaqueToken(t *testing.T) {
	isErrorIs(t, validOpaqueToken(""), ErrInvalidArgument)
	isErrorIs(t, validOpaqueToken("has space"), ErrInvalidArgument)
	isErrorIs(t, validOpaqueToken(string(bytes.Repeat([]byte("a"), 33))), ErrInvalidArgument)
	noError(t, validOpaqueToken("opaque"))
}
