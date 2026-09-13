package memcached

import (
	"testing"
)

func Test_parseMetaItemDebug(t *testing.T) {
	item := &MetaItemDebug{}
	err := parseMetaItemDebug([][]byte{[]byte("ME foo exp=-1 la=2 cas=18 fetch=no cls=1 size=65\r\n")}, item)
	noError(t, err)
	equal(t, []byte("foo"), item.Key)
	equal(t, int64(-1), item.TTL)
	equal(t, uint64(18), item.CAS)
	isErrorIs(t, parseMetaItemDebug([][]byte{[]byte("EN\r\n")}, &MetaItemDebug{}), ErrNotFound)
}

func Test_parseMetaItemDebug_unknownFields(t *testing.T) {
	item := &MetaItemDebug{}
	err := parseMetaItemDebug([][]byte{[]byte("ME foo exp=-1 la=2 cas=18 fetch=no cls=1 size=65 extra=yes path=/x\r\n")}, item)
	noError(t, err)
	equal(t, []byte("foo"), item.Key)
	equal(t, "yes", item.Fields["extra"])
	equal(t, "/x", item.Fields["path"])
	isTrue(t, item.Fields["exp"] == "")
}

func Test_buildMetaDebugCommand_PL(t *testing.T) {
	req, _ := buildMetaDebugCommand([]byte("foo"), &metaDebugFlags{P: "h", L: "/p"})
	equal(t, []byte("me foo Ph L/p\r\n"), req.raw)
}
