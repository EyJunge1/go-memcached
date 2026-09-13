package memcached

import (
	"errors"
	"fmt"
	"testing"
)

func TestWrappedErrorChainPreservesSentinel(t *testing.T) {
	err := fmt.Errorf("request failed: %w", fmt.Errorf("send failed: %w", ErrNotFound))
	isTrue(t, errors.Is(err, ErrNotFound))
	contains(t, err.Error(), "request failed")
	contains(t, err.Error(), "send failed")
}

func TestWrapRecvErrorKeepsProtocolSentinels(t *testing.T) {
	err := wrapRecvError(fmt.Errorf("recv failed: %w", ErrNotFound))
	isErrorIs(t, err, ErrNotFound)
	isFalse(t, isTransportError(err))
	isFalse(t, shouldDiscardConn(err))

	ioErr := wrapRecvError(fmt.Errorf("recv failed: %w", ioTimeoutError{}))
	isTrue(t, isTransportError(ioErr))
	isTrue(t, shouldDiscardConn(ioErr))

	malformed := wrapRecvError(fmt.Errorf("bad frame: %w", ErrMalformedResponse))
	isTrue(t, shouldDiscardConn(malformed))
}

type ioTimeoutError struct{}

func (ioTimeoutError) Error() string { return "i/o timeout" }

func TestTransportSentinel(t *testing.T) {
	err := wrapTransport(fmt.Errorf("dial failed: %w", ioTimeoutError{}))
	isErrorIs(t, err, ErrTransport)
}

func TestErrorsJoinPreservesJoinedSentinels(t *testing.T) {
	joined := errors.Join(
		fmt.Errorf("node-a: %w", ErrNotFound),
		fmt.Errorf("node-b: %w", ErrServerError),
	)
	isError(t, joined)
	isTrue(t, errors.Is(joined, ErrNotFound))
	isTrue(t, errors.Is(joined, ErrServerError))
}
