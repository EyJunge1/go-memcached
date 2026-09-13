package memcached

import (
	"testing"
)

func Test_parseStatsMap(t *testing.T) {
	got, err := parseStatsMap([][]byte{
		[]byte("STAT items:1:number 2\r\n"),
		[]byte("STAT pid 668\r\n"),
		[]byte("END\r\n"),
	})
	noError(t, err)
	equal(t, "2", got["items:1:number"])
	equal(t, "668", got["pid"])
}

func Test_parseStatsDetailDump(t *testing.T) {
	got, err := parseStatsDetailDump([][]byte{
		[]byte("PREFIX foo 1 2 3\r\n"),
		[]byte("PREFIX bar 0 0 1\r\n"),
		[]byte("END\r\n"),
	})
	noError(t, err)
	equal(t, "1 2 3", got["foo"])
	equal(t, "0 0 1", got["bar"])
}

func Test_parseStatsDetailDump_edges(t *testing.T) {
	got, err := parseStatsDetailDump([][]byte{
		[]byte("PREFIX onlykey\r\n"),
		[]byte("STAT pid 7\r\n"),
		[]byte("END\r\n"),
	})
	noError(t, err)
	equal(t, "", got["onlykey"])
	equal(t, "7", got["pid"])

	_, err = parseStatsDetailDump([][]byte{[]byte("CLIENT_ERROR bad detail\r\n")})
	isErrorIs(t, err, ErrClientError)

	_, err = parseStatsDetailDump([][]byte{[]byte("not-a-stat-line\r\n")})
	isErrorIs(t, err, ErrMalformedResponse)
}
