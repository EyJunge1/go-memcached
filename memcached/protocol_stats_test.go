package memcached

import (
	"testing"
)

func Test_parseStatsMap(t *testing.T) {
	var got []Stat
	noError(t, parseStatsInto([][]byte{
		[]byte("STAT items:1:number 2\r\n"),
		[]byte("STAT pid 668\r\n"),
		[]byte("END\r\n"),
	}, &got))
	v, ok := lookupStat(got, "items:1:number")
	isTrue(t, ok)
	equal(t, "2", v)
	v, ok = lookupStat(got, "pid")
	isTrue(t, ok)
	equal(t, "668", v)

	isErrorIs(t, parseStatsInto([][]byte{[]byte("STAT pid 1\r\n")}, nil), ErrInvalidArgument)
	line := []byte("STAT pid 668\r\n")
	reuse := []Stat{{Name: make([]byte, 0, 8), Value: make([]byte, 0, 8)}}
	noError(t, parseStatsInto([][]byte{line, []byte("END\r\n")}, &reuse))
	line[5] = 'X'
	v, ok = lookupStat(reuse, "pid")
	isTrue(t, ok)
	equal(t, "668", v)
	equal(t, 8, cap(reuse[0].Name))
	equal(t, 8, cap(reuse[0].Value))
}

func Test_parseStatsDetailDump(t *testing.T) {
	var got []Stat
	noError(t, parseStatsDetailInto([][]byte{
		[]byte("PREFIX foo 1 2 3\r\n"),
		[]byte("PREFIX bar 0 0 1\r\n"),
		[]byte("END\r\n"),
	}, &got))
	v, ok := lookupStat(got, "foo")
	isTrue(t, ok)
	equal(t, "1 2 3", v)
	v, ok = lookupStat(got, "bar")
	isTrue(t, ok)
	equal(t, "0 0 1", v)
}

func Test_consumeStatsLine(t *testing.T) {
	var got []Stat
	noError(t, consumeStatsLine(&got, []byte("STAT pid 1\r\n"), false))
	noError(t, consumeStatsLine(&got, []byte("PREFIX foo 1 2\r\n"), false))
	noError(t, consumeStatsLine(&got, []byte("END\r\n"), false))
	equal(t, 1, len(got))
	v, ok := lookupStat(got, "pid")
	isTrue(t, ok)
	equal(t, "1", v)

	var detail []Stat
	noError(t, consumeStatsLine(&detail, []byte("PREFIX foo 1 2 3\r\n"), true))
	noError(t, consumeStatsLine(&detail, []byte("STAT pid 7\r\n"), true))
	equal(t, 2, len(detail))
	v, ok = lookupStat(detail, "foo")
	isTrue(t, ok)
	equal(t, "1 2 3", v)

	isErrorIs(t, consumeStatsLine(nil, []byte("STAT pid 1\r\n"), false), ErrInvalidArgument)
	isErrorIs(t, consumeStatsLine(&got, []byte("CLIENT_ERROR bad\r\n"), false), ErrClientError)
}

func Test_parseStatsDetailDump_edges(t *testing.T) {
	var got []Stat
	noError(t, parseStatsDetailInto([][]byte{
		[]byte("PREFIX onlykey\r\n"),
		[]byte("STAT pid 7\r\n"),
		[]byte("END\r\n"),
	}, &got))
	v, ok := lookupStat(got, "onlykey")
	isTrue(t, ok)
	equal(t, "", v)
	v, ok = lookupStat(got, "pid")
	isTrue(t, ok)
	equal(t, "7", v)

	isErrorIs(t, parseStatsDetailInto([][]byte{[]byte("CLIENT_ERROR bad detail\r\n")}, &got), ErrClientError)
	isErrorIs(t, parseStatsDetailInto([][]byte{[]byte("not-a-stat-line\r\n")}, &got), ErrMalformedResponse)
}
