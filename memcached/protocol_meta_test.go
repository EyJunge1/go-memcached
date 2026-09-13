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

func Test_validMetaHints(t *testing.T) {
	noError(t, validMetaHints("", ""))
	noError(t, validMetaHints("hint", "/path"))
	isErrorIs(t, validMetaHints("bad token", ""), ErrInvalidArgument)
	isErrorIs(t, validMetaHints("", "x y"), ErrInvalidArgument)
}
