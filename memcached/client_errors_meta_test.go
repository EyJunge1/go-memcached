package memcached

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

func TestMetaPipelineQuietSetIsHD(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	got, err := doMetaPipeline(context.Background(), client, []MetaOp{
		{Key: []byte("k"), Body: []byte("v"), Set: &MetaSet{}},
	})
	noError(t, err)
	hasLen(t, got, 1)
	equal(t, MetaStatusHD, got[0].Status)
	isNil(t, got[0].Err())
}

func TestMetaQuietBarrierAcceptsLFOnlyMN(t *testing.T) {
	addr, dials := startCmdServer(t, func(fields []string) string {
		switch fields[0] {
		case "mg":
			return ""
		case "mn":
			return "MN\n"
		case "version":
			return "VERSION lf-mn\r\n"
		default:
			return "ERROR\r\n"
		}
	})
	client := testClient(t, addr)
	res, err := doMetaGet(context.Background(), client, []byte("k"), MetaGet{Quiet: true})
	noError(t, err)
	isErrorIs(t, res.Err(), ErrNotFound)
	equal(t, MetaStatusEN, res.Status)
	ver, err := doVersion(context.Background(), client)
	noError(t, err)
	equal(t, []byte("lf-mn"), ver)
	equal(t, int32(1), dials.Load())
}

func TestMetaPipelineChargeKeepsSiblingHD(t *testing.T) {
	addr, _ := startCmdServer(t, func(fields []string) string {
		switch fields[0] {
		case "md":
			return "HD O0\r\n"
		case "mn":
			return "CLIENT_ERROR bad meta\r\nMN\r\n"
		default:
			return "ERROR\r\n"
		}
	})
	client := testClient(t, addr)
	got, err := doMetaPipeline(context.Background(), client, []MetaOp{
		{Key: []byte("k"), Delete: &MetaDelete{}},
	})
	isErrorIs(t, err, ErrClientError)
	hasLen(t, got, 1)
	equal(t, MetaStatusHD, got[0].Status)
	isNil(t, got[0].Err())
}

type quietLineConn struct {
	mockConn

	line []byte
}

func (c *quietLineConn) readLine(byte) ([]byte, error) {
	if len(c.line) == 0 {
		return nil, io.EOF
	}
	line := c.line
	c.line = nil
	return line, nil
}

func (c *quietLineConn) buffered() int { return len(c.line) }

func TestReadQuietFaultBufferedAndEmpty(t *testing.T) {
	err := readQuietFault(context.Background(), &quietLineConn{line: []byte("CLIENT_ERROR bad flags\r\n")})
	isErrorIs(t, err, ErrClientError)
	contains(t, errText(t, err), "bad flags")
	noError(t, readQuietFault(context.Background(), &quietLineConn{}))
}

func TestReadQuietFaultCancelledStillReadsBuffered(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := readQuietFault(ctx, &quietLineConn{line: []byte("CLIENT_ERROR already here\r\n")})
	isErrorIs(t, err, ErrClientError)
	contains(t, errText(t, err), "already here")
	noError(t, readQuietFault(ctx, &quietLineConn{}))

	expired, stop := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer stop()
	err = readQuietFault(expired, &quietLineConn{line: []byte("SERVER_ERROR oom\r\n")})
	isErrorIs(t, err, ErrServerError)
}

func TestMetaPipelineChargeOnlyDoesNotInventHD(t *testing.T) {
	addr, _ := startCmdServer(t, func(fields []string) string {
		switch fields[0] {
		case "md":
			return "CLIENT_ERROR bad meta\r\n"
		case "mn":
			return "MN\r\n"
		default:
			return "ERROR\r\n"
		}
	})
	client := testClient(t, addr)
	got, err := doMetaPipeline(context.Background(), client, []MetaOp{
		{Key: []byte("k"), Delete: &MetaDelete{}},
	})
	isErrorIs(t, err, ErrClientError)
	hasLen(t, got, 1)
	equal(t, MetaStatusUnknown, got[0].Status)
	isErrorIs(t, got[0].Err(), ErrClientError)
	isFalse(t, errors.Is(got[0].Err(), ErrTransport))
}

func TestMetaRejectsWikiSTAndSpecDE(t *testing.T) {
	addr, _ := startCmdServer(t, func(fields []string) string {
		switch fields[0] {
		case "md":
			return "DE\r\n"
		case "ms":
			return "ST\r\n"
		default:
			return "ERROR\r\n"
		}
	})
	client := testClient(t, addr)
	_, err := doMetaDelete(context.Background(), client, []byte("k"), MetaDelete{})
	isErrorIs(t, err, ErrMalformedResponse)
	isTrue(t, shouldDiscardConn(err))
	_, err = doMetaSet(context.Background(), client, []byte("k"), []byte("v"), MetaSet{})
	isErrorIs(t, err, ErrMalformedResponse)
	isTrue(t, shouldDiscardConn(err))
}
