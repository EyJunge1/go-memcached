package memcached

import (
	"testing"
)

func Test_buildAdminCommands_wire(t *testing.T) {
	req, resp := buildVerbosityCommand(2)
	equal(t, []byte("verbosity 2\r\n"), req.raw)
	notNil(t, resp)
	req.release()
	resp.release()

	req, resp = buildShutdownCommand(true)
	equal(t, []byte("shutdown graceful\r\n"), req.raw)
	req.release()
	resp.release()

	req, resp = buildSlabsReassignCommand(-1, 3)
	equal(t, []byte("slabs reassign -1 3\r\n"), req.raw)
	req.release()
	resp.release()

	req, resp = buildLRUCrawlerCommand("crawl", "all")
	equal(t, []byte("lru_crawler crawl all\r\n"), req.raw)
	req.release()
	resp.release()
}

func Test_validCrawlerSelector(t *testing.T) {
	noError(t, validCrawlerSelector("all"))
	noError(t, validCrawlerSelector("hash"))
	noError(t, validCrawlerSelector("1,3,10"))
	isErrorIs(t, validCrawlerSelector("all\r\nwatch"), ErrInvalidArgument)
	isErrorIs(t, validCrawlerSelector("1 2"), ErrInvalidArgument)
	isErrorIs(t, validCrawlerSelector(""), ErrInvalidArgument)
}

func Test_mapAdminStatus(t *testing.T) {
	noError(t, mapAdminStatus([]byte("OK\r\n")))
	isErrorIs(t, mapAdminStatus([]byte("BUSY\r\n")), ErrBusy)
	isErrorIs(t, mapAdminStatus([]byte("SAME\r\n")), ErrSame)
	isErrorIs(t, mapAdminStatus([]byte("BADCLASS\r\n")), ErrBadClass)
}

func Test_adminStreamLineError(t *testing.T) {
	equal(t, error(nil), adminStreamLineError("key=foo exp=-1"))
	isErrorIs(t, adminStreamLineError("BUSY"), ErrBusy)
	isErrorIs(t, adminStreamLineError("BUSY crawler"), ErrBusy)
	isErrorIs(t, adminStreamLineError("ERROR"), ErrNonexistentCommand)
	isErrorIs(t, adminStreamLineError("CLIENT_ERROR bad"), ErrClientError)
}
