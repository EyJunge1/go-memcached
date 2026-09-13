package memcached

import (
	"testing"
)

func Test_parseMetaItemDebug(t *testing.T) {
	item := &MetaItemDebug{}
	err := parseMetaItemDebug([][]byte{[]byte("ME foo exp=-1 la=2 cas=18 fetch=no cls=1 size=65\r\n")}, item)
	noError(t, err)
	equal(t, int64(-1), item.TTL)
	equal(t, uint64(18), item.CAS)
	isErrorIs(t, parseMetaItemDebug([][]byte{[]byte("EN\r\n")}, &MetaItemDebug{}), ErrNotFound)
}
