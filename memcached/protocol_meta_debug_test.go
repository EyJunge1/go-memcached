package memcached

import (
	"testing"
)

func Test_parseMetaItemDebug(t *testing.T) {
	item := &MetaItemDebug{}
	err := parseMetaItemDebug([][]byte{[]byte("ME foo exp=-1 la=2 cas=18 fetch=no cls=1 size=65\r\n")}, item, false)
	noError(t, err)
	equal(t, []byte("foo"), item.Key)
	equal(t, []byte("foo"), item.WireKey)
	isFalse(t, item.KeyBase64)
	equal(t, int64(-1), item.TTL)
	equal(t, uint64(18), item.CAS)
	equal(t, MetaStatusME, item.Status)
	miss := &MetaItemDebug{}
	isErrorIs(t, parseMetaItemDebug([][]byte{[]byte("EN\r\n")}, miss, false), ErrNotFound)
	equal(t, MetaStatusEN, miss.Status)
	isErrorIs(t, parseMetaItemDebug([][]byte{[]byte("ERROR bad args\r\n")}, &MetaItemDebug{}, false), ErrCommandError)
	isErrorIs(t, parseMetaItemDebug([][]byte{[]byte("CLIENT_ERROR bad me\r\n")}, &MetaItemDebug{}, false), ErrClientError)
	isErrorIs(t, parseMetaItemDebug([][]byte{[]byte("XX\r\n")}, &MetaItemDebug{}, false), ErrMalformedResponse)
}

func Test_parseMetaItemDebug_unknownFields(t *testing.T) {
	item := &MetaItemDebug{}
	err := parseMetaItemDebug([][]byte{[]byte("ME foo exp=-1 la=2 cas=18 fetch=no cls=1 size=65 extra=yes path=/x\r\n")}, item, false)
	noError(t, err)
	equal(t, []byte("foo"), item.Key)
	equal(t, "yes", item.Fields["extra"])
	equal(t, "/x", item.Fields["path"])
	isTrue(t, item.Fields["exp"] == "")
}

func Test_parseMetaItemDebug_binaryKey(t *testing.T) {
	raw := []byte("hi")
	wire := base64Encode(raw)
	item := &MetaItemDebug{}
	err := parseMetaItemDebug([][]byte{[]byte("ME " + string(wire) + " exp=-1 la=0 cas=1 fetch=no cls=1 size=2\r\n")}, item, true)
	noError(t, err)
	equal(t, raw, item.Key)
	equal(t, wire, item.WireKey)
	isTrue(t, item.KeyBase64)

	bad := &MetaItemDebug{}
	isErrorIs(t, parseMetaItemDebug([][]byte{[]byte("ME !!! exp=-1 la=0 cas=1 fetch=no cls=1 size=0\r\n")}, bad, true), ErrMalformedResponse)
}

func Test_buildMetaDebugCommand_PL(t *testing.T) {
	req, _ := buildMetaDebugCommand([]byte("foo"), &metaDebugFlags{P: "h", L: "/p"})
	equal(t, []byte("me foo Ph L/p\r\n"), req.raw)
}
