package memcached

import (
	"bytes"
	"testing"
	"time"
)

func Test_validateKeyAndValue(t *testing.T) {
	noError(t, validateKeyAndValue([]byte("key"), []byte("value")))
	noError(t, validateKeyAndValue([]byte("key"), nil))
	isErrorIs(t, validateKeyAndValue(nil, []byte("value")), ErrInvalidKey)
	isErrorIs(t, validateKeyAndValue([]byte(""), []byte("value")), ErrInvalidKey)
	isErrorIs(t, validateKeyAndValue(bytes.Repeat([]byte("a"), maxKeySize+1), nil), ErrInvalidKey)
	isErrorIs(t, validateKeyAndValue([]byte("has space"), nil), ErrInvalidKey)
	isErrorIs(t, validateKeyAndValue([]byte("has\ttab"), nil), ErrInvalidKey)
	isErrorIs(t, validateKeyAndValue([]byte{0x7f}, nil), ErrInvalidKey)
	noError(t, validateKeyAndValueFlags([]byte("has space"), nil, true))
	isErrorIs(t, validateKeyAndValueFlags(bytes.Repeat([]byte("a"), 188), nil, true), ErrInvalidKey)
	noError(t, validateKeyAndValueFlags(bytes.Repeat([]byte("a"), 186), nil, true))
	equal(t, 250, maxKeySize)
}

func Test_expiryToken(t *testing.T) {
	equal(t, int64(0), expiryToken(0))
	equal(t, int64(-1), expiryToken(-time.Second))
	equal(t, int64(1), expiryToken(500*time.Millisecond))
	equal(t, int64(1), expiryToken(time.Second))
	equal(t, int64(60), expiryToken(60*time.Second))

	fixed := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	prev := nowFunc
	nowFunc = func() time.Time { return fixed }
	t.Cleanup(func() { nowFunc = prev })
	want := fixed.Add(31 * 24 * time.Hour).Unix()
	equal(t, want, expiryToken(31*24*time.Hour))
}

func Test_buildStorageCommand_expiryWire(t *testing.T) {
	req, resp := buildStorageCommand("set", "k", []byte("v"), 0, 500*time.Millisecond, false)
	defer releaseReqAndResp(req, resp)
	equal(t, []byte("set k 0 1 1\r\nv\r\n"), req.raw)

	req, resp = buildStorageCommand("set", "k", []byte("v"), 0, -time.Second, false)
	defer releaseReqAndResp(req, resp)
	equal(t, []byte("set k 0 -1 1\r\nv\r\n"), req.raw)
}

func Test_mapStorageReply(t *testing.T) {
	stored := &response{endIndicator: endIndicatorLimitedLines, rawLines: [][]byte{_StoredCRLFBytes}}
	noError(t, mapStorageReply(stored))

	notStored := &response{endIndicator: endIndicatorLimitedLines, rawLines: [][]byte{[]byte("NOT_STORED\r\n")}}
	isErrorIs(t, mapStorageReply(notStored), ErrNotStored)
	isFalse(t, shouldDiscardConn(wrapRecvError(mapStorageReply(notStored))))

	exists := &response{endIndicator: endIndicatorLimitedLines, rawLines: [][]byte{[]byte("EXISTS\r\n")}}
	isErrorIs(t, mapStorageReply(exists), ErrExists)
	isFalse(t, shouldDiscardConn(wrapRecvError(mapStorageReply(exists))))

	notFound := &response{endIndicator: endIndicatorLimitedLines, rawLines: [][]byte{[]byte("NOT_FOUND\r\n")}}
	isErrorIs(t, mapStorageReply(notFound), ErrNotFound)

	junk := &response{endIndicator: endIndicatorLimitedLines, rawLines: [][]byte{[]byte("WHATEVER\r\n")}}
	isErrorIs(t, mapStorageReply(junk), ErrMalformedResponse)
	isTrue(t, shouldDiscardConn(wrapRecvError(mapStorageReply(junk))))

	quiet := &response{endIndicator: endIndicatorNoReply}
	noError(t, mapStorageReply(quiet))
}
