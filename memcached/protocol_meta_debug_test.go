package memcached

import (
	"testing"
	"time"
)

func TestParseMetaItemDebug(t *testing.T) {
	item := &MetaItemDebug{}
	err := parseMetaItemDebug([][]byte{[]byte("ME foo exp=-1 la=2 cas=18 fetch=no cls=1 size=65\r\n")}, item, false)
	noError(t, err)
	equal(t, []byte("foo"), item.Key)
	equal(t, []byte("foo"), item.WireKey)
	isFalse(t, item.KeyBase64)
	equal(t, -1*time.Second, item.TTL)
	equal(t, uint64(18), item.CAS)
	equal(t, MetaStatusME, item.Status)
	miss := &MetaItemDebug{}
	isErrorIs(t, parseMetaItemDebug([][]byte{[]byte("EN\r\n")}, miss, false), ErrNotFound)
	equal(t, MetaStatusEN, miss.Status)
	nf := &MetaItemDebug{}
	isErrorIs(t, parseMetaItemDebug([][]byte{[]byte("NF\r\n")}, nf, false), ErrNotFound)
	equal(t, MetaStatusNF, nf.Status)
	isErrorIs(t, parseMetaItemDebug([][]byte{[]byte("ERROR bad args\r\n")}, &MetaItemDebug{}, false), ErrCommandError)
	isErrorIs(t, parseMetaItemDebug([][]byte{[]byte("CLIENT_ERROR bad me\r\n")}, &MetaItemDebug{}, false), ErrClientError)
	isErrorIs(t, parseMetaItemDebug([][]byte{[]byte("XX\r\n")}, &MetaItemDebug{}, false), ErrMalformedResponse)
}

func TestParseMetaItemDebugUnknownFields(t *testing.T) {
	item := &MetaItemDebug{}
	line := []byte("ME foo exp=-1 la=2 cas=18 fetch=no cls=1 size=65 extra=yes path=/x\r\n")
	err := parseMetaItemDebug([][]byte{line}, item, false)
	noError(t, err)
	equal(t, []byte("foo"), item.Key)
	equal(t, "yes", debugField(*item, "extra"))
	equal(t, "/x", debugField(*item, "path"))
	isTrue(t, debugField(*item, "exp") == "")
}

func TestParseMetaItemDebugRejectsInvalidKnownNumbers(t *testing.T) {
	for _, field := range []string{
		"exp=notint", "la=notint", "cas=notint", "cls=notint", "size=notint",
	} {
		line := []byte("ME foo " + field + "\r\n")
		isErrorIs(t, parseMetaItemDebug([][]byte{line}, &MetaItemDebug{}, false), ErrMalformedResponse)
	}
}

func TestParseMetaItemDebugBinaryKey(t *testing.T) {
	raw := []byte("hi")
	wire := base64Encode(raw)
	item := &MetaItemDebug{}
	line := []byte("ME " + string(wire) + " exp=-1 la=0 cas=1 fetch=no cls=1 size=2\r\n")
	err := parseMetaItemDebug([][]byte{line}, item, true)
	noError(t, err)
	equal(t, raw, item.Key)
	equal(t, wire, item.WireKey)
	isTrue(t, item.KeyBase64)

	bad := &MetaItemDebug{}
	badLine := []byte("ME !!! exp=-1 la=0 cas=1 fetch=no cls=1 size=0\r\n")
	isErrorIs(t, parseMetaItemDebug([][]byte{badLine}, bad, true), ErrMalformedResponse)
}

func TestBuildMetaDebugCommandPL(t *testing.T) {
	req, _ := buildMetaDebugCommand([]byte("foo"), &metaDebugFlags{P: "h", L: "/p"})
	equal(t, []byte("me foo Ph L/p\r\n"), req.raw)
}
